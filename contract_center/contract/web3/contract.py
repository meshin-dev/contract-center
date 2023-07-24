import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Dict

from torch.multiprocessing import Manager
from web3 import Web3, HTTPProvider, WebsocketProvider
from web3._utils.abi import abi_to_signature
from web3._utils.events import get_event_data
from web3.contract import Contract
from web3.middleware import geth_poa_middleware

from contract_center.contract.models.sync import GENESIS_EVENT_NAME_DEFAULT
from contract_center.contract.web3.events import sanitize_events

logger = logging.getLogger(__name__)


class Web3Contract:
    """
    High-level contract for easy interaction with blockchain
    """
    name: str
    contract_address: str
    contract_abi: Dict
    web3_http: Web3
    web3_websocket: Dict[str, Web3] = {}
    http: Contract
    websocket: Dict[str, Contract] = {}

    def __init__(self,
                 name: str,
                 node_http_address: str,
                 node_websocket_address: str,
                 contract_address: str,
                 contract_abi: Union[str, dict]):
        """
        Receives all necessary information to create both websocket and http providers,
        contracts and web3 instances
        :param node_http_address:
        :param node_websocket_address:
        :param contract_address:
        :param contract_abi:
        """
        self.name = name
        self.contract_address = contract_address
        self.contract_abi = json.loads(contract_abi) if isinstance(contract_abi, str) else contract_abi
        self.executor = None

        if not self.contract_abi:
            raise ValueError('Provide either string or dict ABI for contract')

        # Web3 instances
        self.web3_http = Web3(HTTPProvider(node_http_address))
        self.web3_websocket[self.name] = Web3(WebsocketProvider(node_websocket_address))

        # Only necessary for PoA chains
        self.web3_http.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.web3_websocket[self.name].middleware_onion.inject(geth_poa_middleware, layer=0)

        # Contract instances
        self.http = self.web3_http.eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=self.contract_abi
        )
        Web3Contract.websocket[self.name] = self.web3_websocket[self.name].eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=self.contract_abi
        )

    def get_genesis_block_number(
        self,
        block_from: int = 0,
        source: str = 'http',
        genesis_event_name: str = GENESIS_EVENT_NAME_DEFAULT
    ) -> Union[None, int]:
        """
        Tries to get the block number when the contract has been initialized.
        :return:
        """
        source = Web3Contract.websocket[self.name] if source == 'websocket' else self.http
        past_events = getattr(source.events, genesis_event_name).get_logs(fromBlock=block_from)
        first_event = past_events[0] if past_events else None
        return int(first_event['blockNumber']) if first_event else None

    @staticmethod
    def estimate_next_chunk_size(
        current_chunk_size: int,
        event_found_count: int,
        min_scan_chunk_size: int = 10,
        max_scan_chunk_size: int = 1000
    ) -> int:
        """
        Estimate the optimal chunk size based on the event density in the current chunk.

        * As more data exists in a smaller chunk, we should request a smaller size next time.
        * As less data exists in a larger chunk, we should request a larger size next time.
        * The aim is to balance the chunk size to skip empty chunks faster and work with smaller chunks when there's more data.
        * This is done by halving the chunk size when event density is above 0.3 and doubling it when event density is below 0.1.
        * Chunk size is capped at the min and max scan chunk sizes.
        """

        # Calculate event density
        event_density = event_found_count / current_chunk_size

        # Adjust chunk size based on event density
        if event_density > 0.3:
            next_chunk_size = current_chunk_size / 2
        elif event_density < 0.1:
            next_chunk_size = current_chunk_size * 2
        else:
            next_chunk_size = current_chunk_size

        # Cap chunk size at the min and max scan chunk sizes
        next_chunk_size = max(min_scan_chunk_size, next_chunk_size)
        next_chunk_size = min(max_scan_chunk_size, next_chunk_size)

        return int(math.ceil(next_chunk_size))

    def get_logs(
        self,
        block_from: int,
        block_to: int,
        source: str = 'http',
        *args,
        **kwargs,
    ):
        # Define the filter parameters
        filter_params = {
            "fromBlock": block_from,
            "toBlock": block_to,
            "address": self.contract_address
        }

        # Create the filter
        web3 = self.web3_websocket[self.name] if source == 'websocket' else self.web3_http
        filters = web3.eth.filter(filter_params)

        # Fetch the logs
        try:
            logs = filters.get_all_entries()
        except Exception as e:
            logger.error(f'Failed to fetch logs for {self.name} contract. '
                         f'Filters: {filter_params}. Retrying...')
            logger.exception(e)
            raise e
        return sanitize_events([self.decode_log(raw_log) for raw_log in logs or []])

    def event_fetch(
        self,
        event: str,
        block_from: int,
        block_to: int,
        max_retries: int = 3,
        retry_delay: int = 1,
        source: str = 'http'
    ):
        """
        Fetches for specific event in range of blocks. If the attempt to fetch logs fails,
        it will retry fetching a specified number of times with a delay of one second between retries.

        :param event: Name of the event for which logs are being fetched.
        :param block_from: Starting block number for the range of blocks.
        :param block_to: Ending block number for the range of blocks.
        :param max_retries: Maximum number of retries if fetching fails. Defaults to 3.
        :param retry_delay: How long to wait before next retry. Defaults to 1 second
        :param source: Use websocket by default or http source to get the data

        :return: A list of event logs fetched from the specified range of blocks.

        :raises: The last exception caught if all retries fail.
        """
        source = Web3Contract.websocket[self.name] if source == 'websocket' else self.http
        while max_retries:
            try:
                logger.debug(f'Loading history for event: {event} in range: {block_from}-{block_to} ...')
                return getattr(source.events, event).get_logs(
                    fromBlock=block_from,
                    toBlock=block_to,
                )
            except:
                time.sleep(retry_delay)
                max_retries -= 1
                if max_retries <= 0:
                    raise

    def events_fetch(
        self,
        events: List[str],
        block_from: int,
        block_to: int,
        max_retries: int = 3,
        retry_delay: int = 1,
        default_max_workers: int = 5,
        source: str = 'http'
    ) -> List[Dict]:
        """
        Fetches specific list of events blocks range using multi-threading.

        This method creates a thread-safe list to store all events, and uses a ThreadPoolExecutor to fetch events
        in parallel. If an exception occurs during the process, the executor is shut down gracefully.

        The max_workers for the ThreadPoolExecutor is the minimum of 5 and the number of events.

        :param events: A list of events to fetch.
        :param block_from: The starting block number to fetch events from.
        :param block_to: The ending block number to fetch events to.
        :param max_retries: The maximum number of retries if fetching fails. Defaults to 3.
        :param retry_delay: The delay in seconds between each retry. Defaults to 1.
        :param default_max_workers: The maximum number of parallel fetch processes to use by default
        :param source: Use websocket by default or http source to get the data

        :return: A list of all fetched events.

        :raises: Any exceptions that occur during the fetching process.

        Note: As this method involves I/O operations, consider adjusting the retry_delay and max_retries parameters
        based on your network stability and the load on the server you are fetching events from.
        """
        # Thread-safe list
        manager = Manager()
        all_events = manager.list()
        max_workers = min(default_max_workers, math.ceil(len(events) / 3))

        if not self.executor:
            self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Multi-thread events fetching
        events_result_future = {
            self.executor.submit(
                self.event_fetch,
                **dict(
                    event=event,
                    block_from=block_from,
                    block_to=block_to,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    source=source,
                )
            ) for event in events
        }
        for future in as_completed(events_result_future):
            all_events.extend(future.result())

        return sanitize_events(list(all_events))

    def decode_log(self, log: Dict, event_type: str = None) -> Union[Dict, None]:
        """
        Decodes a log entry using the contract ABI.
        :param log:
        :param event_type:
        :return:
        """
        for event in self.contract_abi:
            if event_type and event.get('type') != event_type:
                continue
            if not event.get('name'):
                continue
            # Look for the correct event by comparing the event signature
            event_signature_str = abi_to_signature(event)
            event_signature = self.web3_http.keccak(text=event_signature_str)
            if log['topics'][0].hex() == event_signature.hex():
                return get_event_data(self.web3_http.codec, event, log)
        return None

    def shutdown(self):
        if self.executor:
            self.executor.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def __del__(self):
        self.shutdown()
