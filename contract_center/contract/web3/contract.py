import json
from typing import Union

from web3 import Web3, HTTPProvider, WebsocketProvider
from web3.contract import Contract


class Web3Contract:
    """
    High-level contract for easy interaction with blockchain
    """
    http_web3: Web3
    websocket_web3: Web3
    http_contract: Contract
    websocket_contract: Contract

    def __init__(self,
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
        abi = json.loads(contract_abi) if isinstance(contract_abi, str) else contract_abi

        if not abi:
            raise ValueError('Provide either string or dict ABI for contract')

        # Web3 instances
        self.http_web3: Web3 = Web3(HTTPProvider(node_http_address))
        self.websocket_web3: Web3 = Web3(WebsocketProvider(node_websocket_address))

        # Contract instances
        self.http_contract = self.http_web3.eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=abi
        )
        self.websocket_contract = self.websocket_web3.eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=abi
        )

    def get_genesis_block_number(self) -> Union[None, int]:
        """
        Tries to get the block number when the contract has been initialized.
        :return:
        """
        past_events = self.http_contract.events.Initialized.get_logs(fromBlock=0)
        first_event = past_events[0] if past_events else None
        return int(first_event['blockNumber']) if first_event else None
