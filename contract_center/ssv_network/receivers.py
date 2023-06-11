import logging
from typing import Dict, Type, List

from django.dispatch import receiver

from contract_center.contract.models import Sync
from contract_center.contract.receivers import ContractEventsReceiverResult
from contract_center.contract.signals import contract_fetched_events_signal
from contract_center.ssv_network.models.events import event_models, EventModel

logger = logging.getLogger(__name__)


@receiver(contract_fetched_events_signal)
def save_raw_events(sender, context: str, sync: Sync, events: List[Dict], **kwargs) -> ContractEventsReceiverResult:
    """
    [
  "OperatorAdded",
  "OperatorRemoved",
  "OperatorFeeExecuted",
  "OperatorFeeDeclared",
  "ClusterLiquidated",
  "ClusterReactivated",
  "ValidatorAdded",
  "ValidatorRemoved",
  "ClusterDeposited",
  "ClusterWithdrawn",
  "FeeRecipientAddressUpdated"
]
    0x45B831727DC96035e6a2f77AAAcE4835195a54Af
    https://eth-goerli.g.alchemy.com/v2/rI4bIEGveSkw0KYAYO8VMIuMJA0QtNIA
    wss://eth-goerli.g.alchemy.com/v2/rI4bIEGveSkw0KYAYO8VMIuMJA0QtNIA

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

    logger.debug(f'Found model for version {version} and network {network}: {event_model}')

    for event in events:
        _, created = event_model.objects.get_or_create(
            transactionHash=event['transactionHash'],
            defaults=dict(
                version=version,
                network=network,
                **event,
            )
        )
        if created:
            result.saved_events[event['event']] = result.saved_events.get(event['event']) or 0
            result.saved_events[event['event']] += 1

            # TODO: start raw events processor celery task specific to this module
            # TODO: that celery task should work 1 instance at a time only specifically for ssv.network events

            # Save last synced block number after each raw event saved.
            # Do it anyway so that on a next raw events fetch task it will continue from
            # the place where it was saved successfully last time
            sync.last_synced_block_number = event['blockNumber'] - 1
            sync.save()
            result.saved_total += 1
            logger.info(f'{sync.name} [{context}]: Saved new event: {event}')

    return result
