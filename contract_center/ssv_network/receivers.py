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

    event_model: Type[EventModel] = get_event_model(version, network)
    assert event_model, f'EventModel for version {version} and network {network} is not found'

    instance.log(f'Found model for version {version} and network {network}: {event_model}', log_method=logger.debug)

    at_least_one_created = False
    last_time = time.time() - 1
    for event in events:
        _, created = event_model.objects.get_or_create(
            transactionHash=event['transactionHash'],
            defaults=dict(
                version=version,
                network=network,
                **event,
            )
        )
        at_least_one_created = at_least_one_created or created
        if created:
            result.saved_events[event['event']] = result.saved_events.get(event['event']) or 0
            result.saved_events[event['event']] += 1

            # Save last synced block number after each raw event saved.
            # Do it anyway so that on a next raw events fetch task it will continue from
            # the place where it was saved successfully last time
            instance.sync.last_synced_block_number = event['blockNumber']
            instance.sync.save()

            result.saved_total += 1
            instance.log(
                f'Saved new raw event: {event}',
                log_method=logger.debug
            )

            # Trigger periodic task to process events at least once every second
            if time.time() - last_time >= 1:
                instance.log(
                    f'Triggering event processing task..',
                    log_method=logger.debug
                )
                EventsProcessTask(
                    kwargs=dict(
                        version=version,
                        network=network
                    )
                ).send()
                last_time = time.time()

    # Trigger anyway in the end task to process events
    if at_least_one_created:
        EventsProcessTask(
            kwargs=dict(
                version=version,
                network=network
            )
        ).send()

    instance.sync.last_synced_block_number = params.get('block_to')
    instance.sync.save()

    return result
