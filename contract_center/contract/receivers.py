import time
from typing import Dict, Type, List
from django.dispatch import receiver
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from contract_center.contract.models import Sync
from contract_center.contract.signals import contract_new_events_signal
from contract_center.ssv_network.models.events import event_models, EventModel


@dataclass_json
@dataclass
class ContractEventsReceiverResult:
    receiver: str
    saved_total: int = 0
    saved_events: dict = field(default_factory=dict)


@receiver(contract_new_events_signal)
def save_raw_events(sender, context: str, sync: Sync, events: List[Dict], **kwargs) -> ContractEventsReceiverResult:
    """
    TODO: identify which version and network it should be in ssv.network module
          and save events in a specific for this module and sync meta table
    :param context:
    :param events:
    :param sender:
    :param sync:
    :param kwargs:
    :return:
    """
    # Not going to save events for other than ssv.network modules
    result = ContractEventsReceiverResult(
        receiver=f'{__name__}.{save_raw_events.__name__}'
    )
    if not sync.name.startswith('ssv_network'):
        return result

    version = str(sync.meta.get('version'))
    network = str(sync.meta.get('network'))
    event_model: Type[EventModel] = event_models.get(f'{version}_{network}')

    for event in events:
        _, created = event_model.objects.get_or_create(
            transactionHash=event['transactionHash'],
            defaults=dict(
                version=version,
                network=network,
                **event,
            )
        )
        time.sleep(1)  # TODO: emulating long lasting process. remove
        if created:
            result.saved_events[event['event']] = result.saved_events.get(event['event']) or 0
            result.saved_events[event['event']] += 1

            sync.last_synced_block_number = event['blockNumber']
            sync.save()

            # TODO: start raw events processor celery task specific to this module

            result.saved_total += 1
            print(f'{sync.name} [{context}]: Saved new event: {event}')

    return result
