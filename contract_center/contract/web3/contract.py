import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Dict

from torch.multiprocessing import Manager
from web3 import Web3, HTTPProvider, WebsocketProvider
from web3.contract import Contract

from contract_center.contract.models.sync import GENESIS_EVENT_NAME_DEFAULT
from contract_center.contract.web3.events import sanitize_events

logger = logging.getLogger(__name__)


class Web3Contract:
    """
    High-level contract for easy interaction with blockchain
    """
    name: str
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
        abi = json.loads(contract_abi) if isinstance(contract_abi, str) else contract_abi

        if not abi:
            raise ValueError('Provide either string or dict ABI for contract')

        # Web3 instances
        self.web3_http = Web3(HTTPProvider(node_http_address))
        self.web3_websocket[self.name] = Web3(WebsocketProvider(node_websocket_address))

        # Contract instances
        self.http = self.web3_http.eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=abi
        )
        Web3Contract.websocket[self.name] = self.web3_websocket[self.name].eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=abi
        )

    def get_genesis_block_number(self, block_from: int = 0, source: str = 'http',
                                 genesis_event_name: str = GENESIS_EVENT_NAME_DEFAULT) -> Union[None, int]:
        """
        Tries to get the block number when the contract has been initialized.
        :return:
        """
        source = Web3Contract.websocket[self.name] if source == 'websocket' else self.http
        past_events = getattr(source.events, genesis_event_name).get_logs(fromBlock=block_from)
        first_event = past_events[0] if past_events else None
        return int(first_event['blockNumber']) if first_event else None

    def event_fetch(self, event: str, block_from: int, block_to: int, max_retries: int = 3, retry_delay: int = 1,
                    source: str = 'http'):
        """
        Fetches event logs from a specific range of blocks for a given event name. If the attempt to fetch logs fails,
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

    def events_fetch(self, events: List[str], block_from: int, block_to: int, max_retries: int = 3,
                     retry_delay: int = 1, default_max_workers: int = 5, source: str = 'http') -> List[Dict]:
        """
        Fetches all events for the specified blocks using multi-threading.

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
        max_workers = min(default_max_workers, len(list(events)))

        # Multi-thread events fetch
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            events_result_future = {
                executor.submit(
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
            try:
                for future in as_completed(events_result_future):
                    all_events.extend(future.result())
            except:
                # Graceful shutdown thread pool
                executor.shutdown()
                raise
        return sanitize_events(list(all_events))
