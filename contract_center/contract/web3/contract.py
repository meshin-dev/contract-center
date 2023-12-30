import hashlib
import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from typing import Union, List, Dict, NewType, Type

from eth_typing import ChecksumAddress
from torch.multiprocessing import Manager
from web3 import Web3, HTTPProvider, WebsocketProvider
from web3._utils.abi import abi_to_signature
from web3._utils.events import get_event_data
from web3.contract import Contract
from web3.middleware import geth_poa_middleware

from contract_center.contract.models.sync import GENESIS_EVENT_NAME_DEFAULT
from contract_center.contract.web3.events import sanitize_events

logger = logging.getLogger(__name__)


class ConnectionType(Enum):
    HTTP = 'HTTP'
    WEBSOCKET = 'WEBSOCKET'


ContractUID = NewType('ContractUID', str)


class Web3Contract:
    """
    High-level Web3 contract interface for easy interaction with blockchain.
    TODO: When using this class in periodic tasks - great idea would be to build registry of them,
          to keep websocket connection alive instead of instantiating it per periodic task
    """
    # Node addresses
    node_http_address: str = ''
    node_websocket_address: str = ''

    # Web3 instances
    web3_http: Union[None, Web3] = None
    web3_websocket: Dict[ContractUID, Web3] = dict()

    # Contract settings
    contract_abi: Dict = dict()
    contract_address: Union[None, ChecksumAddress] = None

    # Contract instances
    http_contract: Union[None, Union[Type[Contract], Contract]] = None
    websocket_contract: Dict[ContractUID, Union[Type[Contract], Contract]] = dict()

    # Executor
    executor: Union[None, ThreadPoolExecutor] = None

    def set_node_http_address(self, node_http_address: str) -> 'Web3Contract':
        """
        Setting node http address
        :param node_http_address:
        :return:
        """
        self.node_http_address = node_http_address
        self.connect(force_http=True)
        return self

    def set_node_websocket_address(self, node_websocket_address: str) -> 'Web3Contract':
        """
        Setting node websocket address
        :param node_websocket_address:
        :return:
        """
        self.node_websocket_address = node_websocket_address
        self.connect(force_websocket=True)
        return self

    @property
    def contract_uid(self) -> ContractUID:
        """
        Build unique contract instance hash based on address and ABI
        :return:
        """
        if not self._contract_uid:
            self._contract_uid = ContractUID(
                hashlib.sha256(
                    str(self.contract_abi).encode() + str(self.contract_address).encode()
                ).hexdigest()
            )
        return self._contract_uid

    def connect(self, force: bool = False, force_http: bool = False, force_websocket: bool = False) -> 'Web3Contract':
        """
        Create instances of Web3 http and websocket only if not created yet or if forced
        :param force_websocket:
        :param force_http:
        :param force:
        :return:
        """
        if all([self.node_http_address, any([force, force_http, not self.web3_http])]):
            self.web3_http = Web3(HTTPProvider(self.node_http_address))
            # Only necessary for PoA chains
            self.web3_http.middleware_onion.inject(geth_poa_middleware, layer=0)

        web3_socket = Web3Contract.web3_websocket.get(self.contract_uid)
        if all([self.node_websocket_address, any([force, force_websocket, not web3_socket])]):
            Web3Contract.web3_websocket[self.contract_uid] = Web3(WebsocketProvider(self.node_websocket_address))
            # Only necessary for PoA chains
            Web3Contract.web3_websocket[self.contract_uid].middleware_onion.inject(geth_poa_middleware, layer=0)
        return self

    @property
    def websocket_connection(self) -> Union[None, Web3]:
        """
        Creates web3 websocket connection
        :return:
        """
        self.connect()
        return Web3Contract.web3_websocket.get(self.contract_uid)

    @property
    def http_connection(self) -> Union[None, Web3]:
        """
        Creates web3 http connection
        :return:
        """
        self.connect()
        return self.web3_http

    def connection(self, connection_type: ConnectionType = ConnectionType.WEBSOCKET) -> Web3:
        """
        Creates web3 instance based on connection type
        :param connection_type:
        :return:
        """
        connections = {
            ConnectionType.HTTP: self.http_connection,
            ConnectionType.WEBSOCKET: self.websocket_connection,
        }
        connection = connections.get(connection_type)
        if not connection:
            raise ValueError(f'This connection type unavailable. Provide {str(connection_type.value).lower()} address')
        return connection

    def contract(self, connection_type: ConnectionType = ConnectionType.WEBSOCKET) -> Contract:
        """
        Returns contract instance based on http or websocket web3 provider connection
        :param connection_type:
        :return:
        """
        # Websocket provider contract
        if connection_type == ConnectionType.WEBSOCKET:
            if Web3Contract.websocket_contract.get(self.contract_uid):
                return Web3Contract.websocket_contract.get(self.contract_uid)
            else:
                contract = self.connection(connection_type=connection_type).eth.contract(
                    address=self.contract_address,
                    abi=self.contract_abi,
                )
                Web3Contract.websocket_contract[self.contract_uid] = contract
                return Web3Contract.websocket_contract.get(self.contract_uid)

        # Http provider contract
        if not self.http_contract:
            self.http_contract = self.connection(connection_type=connection_type).eth.contract(
                address=self.contract_address,
                abi=self.contract_abi,
            )
        return self.http_contract

    def __init__(self, contract_address: str, contract_abi: Union[str, dict], node_http_address: str = None,
                 node_websocket_address: str = None):
        """
        Receives all necessary information to create both websocket and http providers,
        contracts and web3 instances
        :param node_http_address:
        :param node_websocket_address:
        :param contract_address:
        :param contract_abi:
        """
        self._contract_uid = ''
        contract_abi = json.loads(contract_abi) if isinstance(contract_abi, str) else contract_abi

        if not contract_abi:
            raise ValueError('Contract ABI should be either JSON string or python dict')
        self.contract_abi = contract_abi

        if not Web3.is_checksum_address(contract_address):
            raise ValueError('Contract address should be valid checksum address')
        self.contract_address = Web3.to_checksum_address(contract_address)

        # Set available addresses and connect
        self.set_node_http_address(node_http_address)
        self.set_node_websocket_address(node_websocket_address)

    def get_genesis_block_number(
        self,
        block_from: int = 0,
        connection_type: ConnectionType = ConnectionType.HTTP,
        genesis_event_name: str = GENESIS_EVENT_NAME_DEFAULT
    ) -> Union[None, int]:
        """
        Tries to get the block number when the contract has been initialized.
        :return:
        """
        contract = self.contract(connection_type=connection_type)
        past_events = getattr(contract.events, genesis_event_name).get_logs(fromBlock=block_from)
        first_event = past_events[0] if past_events else None
        return int(first_event['blockNumber']) if first_event else None

    def get_logs(
        self,
        block_from: int,
        block_to: int,
        connection_type: ConnectionType = ConnectionType.HTTP,
        *args,
        **kwargs,
    ):
        """
        Get logs between two block numbers for contract address using filters builder
        :param block_from:
        :param block_to:
        :param connection_type:
        :param args:
        :param kwargs:
        :return:
        """
        # Define the filter parameters
        filter_params = {
            "fromBlock": block_from,
            "toBlock": block_to,
            "address": self.contract_address
        }

        # Create the filter
        connection = self.connection(connection_type=connection_type)
        filters = connection.eth.filter(filter_params)

        # Fetch the logs
        try:
            logs = filters.get_all_entries()
        except ValueError as e:
            error_details = e.args[0]
            error_code = error_details.get('code')
            # @url https://eips.ethereum.org/EIPS/eip-1474
            if error_code == -32000 or 'filter not found' in str(error_details):
                # Recreate the filter
                filters = connection.eth.filter(filter_params)
                logs = filters.get_all_entries()
            else:
                raise
        return sanitize_events([self.decode_log(raw_log) for raw_log in logs or []])

    def event_fetch(
        self,
        event: str,
        block_from: int,
        block_to: int,
        max_retries: int = 3,
        retry_delay: int = 1,
        connection_type: ConnectionType = ConnectionType.HTTP
    ):
        """
        Fetches for specific event in range of blocks. If the attempt to fetch logs fails,
        it will retry fetching a specified number of times with a delay of one second between retries.

        :param event: Name of the event for which logs are being fetched.
        :param block_from: Starting block number for the range of blocks.
        :param block_to: Ending block number for the range of blocks.
        :param max_retries: Maximum number of retries if fetching fails. Defaults to 3.
        :param retry_delay: How long to wait before next retry. Defaults to 1 second
        :param connection_type: Use websocket by default or http source to get the data

        :return: A list of event logs fetched from the specified range of blocks.

        :raises: The last exception caught if all retries fail.
        """
        contract = self.contract(connection_type=connection_type)
        while max_retries:
            try:
                logger.debug(f'Loading history for event: {event} in range: {block_from}-{block_to} ...')
                return getattr(contract.events, event).get_logs(
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
        source: str = ConnectionType.HTTP
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
