import json
from web3.types import HexBytes
from typing import List, Union, Dict
from web3.datastructures import AttributeDict


class HexBytesEncoder(json.JSONEncoder):
    """
    Universal parser for nested binary data structures of events
    """
    def default(self, obj):
        if isinstance(obj, bytes):
            return bytes(obj).hex()
        if isinstance(obj, HexBytes):
            return obj.hex()
        if isinstance(obj, AttributeDict):
            return obj.__dict__
        return super().default(obj)


def sanitize_events(events: List[Union[Dict, AttributeDict]]) -> List[Dict]:
    """
    Properly decode each type of event data.
    Sort them by block number and transaction index inside the same block number
    to keep organic chronology.
    It is useful in case when few types of events fetched separately and then processed in chronological order.
    :param events:
    :return:
    """
    events = sorted(events, key=lambda log: (log['blockNumber'], log['transactionIndex']))
    return [json.loads(json.dumps(event, cls=HexBytesEncoder)) for event in events]
