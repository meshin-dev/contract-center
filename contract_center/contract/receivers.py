from dataclasses import dataclass, field

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class ContractEventsReceiverResult:
    """
    Unified structure to return from signal handlers when syncing raw contract events
    """
    receiver: str
    saved_total: int = 0
    saved_events: dict = field(default_factory=dict)
