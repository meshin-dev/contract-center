import logging
import time
from typing import Dict, Type, List

from django.dispatch import receiver

from contract_center.contract.receivers import EventReceiverResult
from contract_center.contract.signals import contract_fetched_events_signal
from contract_center.contract.tasks import EventsFetchTask
from contract_center.ssv_network.models.events import get_event_model, EventModel
from contract_center.ssv_network.tasks import EventsProcessTask

logger = logging.getLogger(__name__)


@receiver(contract_fetched_events_signal)
def contract_fetched_events_receiver(
    sender: Type[EventsFetchTask],
    instance: EventsFetchTask,
    params: Dict,
    events: List[Dict],
    **kwargs
) -> EventReceiverResult:
    """
    Saving raw events from a contract.
    When events are saved - it triggers processing events task.

    :param sender:
    :param instance:
    :param params:
    :param events:
    :param kwargs:
    :return:
    """
    # Not going to save events for other than ssv.network modules
    result = EventReceiverResult(
        receiver=f'{__name__}.{contract_fetched_events_receiver.__name__}'
    )
    if not instance.sync.name.startswith('ssv_network'):
        return result

    version = str(instance.sync.meta.get('version'))
    assert version, 'Version in Sync.meta is required'

    network = str(instance.sync.meta.get('network'))
    assert network, 'Network in Sync.meta is required'

    event_model: Type[EventModel] = get_event_model(network)
    assert event_model, f'EventModel for version {version} and network {network} is not found'

    logger.debug(f'Found model for version {version} and network {network}: {event_model}. '
                 f'Task: {instance.name}. '
                 f'Context: {instance.context}. '
                 f'Lock: {instance.get_lock_name()}')

    at_least_one_created = False
    last_time = time.time() - 1
    most_recent_block_number = None
    for event in events:
        _, created = event_model.objects.get_or_create(
            transactionHash=event['transactionHash'],
            version=version,
            network=network,
            data_version=instance.sync.sync_data_version,
            defaults=dict(
                version=version,
                network=network,
                data_version=instance.sync.sync_data_version,
                **event,
            )
        )

        # Save last synced block number after each raw event saved.
        # Do it anyway so that on a next raw events fetch task it will continue from
        # the place where it was saved successfully last time
        at_least_one_created = at_least_one_created or created
        most_recent_block_number = event['blockNumber']
        if instance.sync.last_synced_block_number < most_recent_block_number:
            instance.sync.last_synced_block_number = most_recent_block_number
            instance.sync.save(update_fields=['last_synced_block_number'])

        if created:
            result.saved_events[event['event']] = result.saved_events.get(event['event']) or 0
            result.saved_events[event['event']] += 1
            result.saved_total += 1
            logger.debug(f'Saved raw event for version {version} and network {network}: {event}. '
                         f'Task: {instance.name}. '
                         f'Context: {instance.context}. '
                         f'Lock: {instance.get_lock_name()}')

            # Trigger periodic task to process events at least once every second
            if time.time() - last_time >= 1:
                logger.debug(f'Triggering event processing task.. '
                             f'Task: {instance.name}. '
                             f'Context: {instance.context}. '
                             f'Lock: {instance.get_lock_name()}')
                EventsProcessTask().apply_async(
                    kwargs=dict(
                        name=instance.sync.name
                    )
                )
                last_time = time.time()

    # Trigger anyway in the end task to process events
    if at_least_one_created:
        EventsProcessTask().apply_async(
            kwargs=dict(
                name=instance.sync.name
            )
        )

    # If there was any data - save the higher block number
    if most_recent_block_number and instance.sync.last_synced_block_number < most_recent_block_number:
        instance.sync.last_synced_block_number = most_recent_block_number
        instance.sync.save(update_fields=['last_synced_block_number'])

    return result
