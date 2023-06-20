import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Type, List

from dataclasses_json import dataclass_json
from django.core.cache import caches
from django.db.models import Q
from django_redis.client import DefaultClient
from redis.lock import Lock

from config import celery_app
from config.celery_app import app
from contract_center.contract.models import Sync
from contract_center.contract.receivers import EventReceiverResult
from contract_center.ssv_network.models.events import get_event_model, EventModel
from contract_center.ssv_network.signals import process_event_signal

logger = logging.getLogger(__name__)


def reacquire_lock(future: Future, lock: Lock, context: str):
    while True:
        if future.done():
            # Stop reacquiring the lock if the future has a result
            break
        try:
            lock.reacquire()
            logger.debug(f'{context}: Reacquired lock during long lasting synchronization...')
        except Exception as e:
            raise
        # Sleep for 5 seconds before reacquiring the lock
        time.sleep(5)


class ProcessStatus(Enum):
    NEW = ''


def fetch_events_callable(event_model: Type[EventModel], process_status: ProcessStatus = None):
    return event_model.objects.filter(Q(process_status=process_status or ProcessStatus.NEW)) \
        .order_by('blockNumber', 'transactionIndex')[:10]


@dataclass_json
@dataclass
class EventsProcessTask:
    """
    For using across different modules
    """
    name: str = 'ssv_network.events_process'
    queue: str = 'queue_events_process'
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)

    def send(self, use_async: bool = True) -> dict:
        if use_async:
            kwargs = self.kwargs
            kwargs.pop('name', None)
            kwargs.pop('queue', None)
            events_process_task.apply_async(args=self.args, kwargs=kwargs)
        else:
            app.send_task(**self.to_dict())
        return self.to_dict()


@dataclass_json
@dataclass
class EventsProcessTaskResult:
    """
    Dataclass representing the result of the EventsProcessTask.
    """
    version: str
    network: str
    lock: str
    reason: str = ''
    error: str = ''
    processed_events: int = 0


@celery_app.task(
    bind=True,
    name=EventsProcessTask.name,
    queue=EventsProcessTask.queue,
)
def events_process_task(self, *args, **kwargs) -> EventsProcessTaskResult:
    """
    Task for processing events asynchronously.

    :param self: The reference to the task instance.
    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    :return: An instance of ProcessEventsTaskResult containing the task execution result.
    """

    # Getting default cache
    cache: DefaultClient = caches['default']
    version = kwargs.pop('version')
    network = kwargs.pop('network')
    lock_name = f'{EventsProcessTask.name}_{version.lower()}_{network.lower()}'
    lock: Lock = cache.lock(lock_name, timeout=15)

    try:
        if lock.locked():
            return EventsProcessTaskResult(
                version=version,
                network=network,
                lock=lock_name,
                reason='Processor is already locked'
            ).to_dict()
        if not lock.acquire(blocking_timeout=5):
            return EventsProcessTaskResult(
                version=version,
                network=network,
                lock=lock_name,
                reason='Could not acquire the lock during 5 seconds'
            ).to_dict()

        # Find appropriate sync info
        sync = Sync.load(meta__version=version, meta__network=network, enabled=True)
        if not sync:
            return EventsProcessTaskResult(
                version=version,
                network=network,
                lock=lock_name,
                reason='Sync is not enabled or not created'
            ).to_dict()

        # Don't count the time spent on database interaction
        lock.reacquire()

        # Get proper model to get events
        event_model: Type[EventModel] = get_event_model(version, network)
        if not event_model:
            raise Exception(f'{self.name} {version} {network}: Could not find event model')

        # Get events to process in a thread
        executor = ThreadPoolExecutor(max_workers=2)
        events_to_process_feature = executor.submit(lambda: fetch_events_callable(event_model))

        # Reacquire the lock in parallel
        reacquire_lock(
            future=events_to_process_feature,
            lock=lock,
            context=f'{self.name} {version} {network}',
        )

        # Check for errors from signal receivers and return an error in case of failure
        error = events_to_process_feature.exception()
        if error:
            # Return error from the task
            return EventsProcessTaskResult(
                version=version,
                network=network,
                lock=lock_name,
                error=f'Could not fetch new events to process: {error} {error.args}'
            ).to_dict()

        # Get events to process
        events: List[EventModel] = events_to_process_feature.result()
        logger.info(f'{self.name} {version} {network}: Fetched {len(events)} events to process')

        # Iterate over events and process them
        for event in events:
            # Trigger signal to process event
            executor = ThreadPoolExecutor(max_workers=2)
            process_event_future = executor.submit(lambda: process_event_signal.send(
                sender=event_model,
                version=version,
                network=network,
                event=event
            ))

            # Reacquire the lock in parallel
            reacquire_lock(
                future=process_event_future,
                lock=lock,
                context=f'{self.name} {version} {network}'
            )

            # Check for errors from signal receivers and return an error in case of failure
            error = process_event_future.exception()
            if error:
                # Return error from the task
                return EventsProcessTaskResult(
                    version=version,
                    network=network,
                    lock=lock_name,
                    error=f'Could not send event to process: {error} {error.args}'
                ).to_dict()

            # Collect receivers' results
            results: List[EventReceiverResult] = process_event_future.result()
            if not len(results):
                logger.warning(f'{self.name} {version} {network}: No results from receivers. '
                               f'Looks like no subscribers to process events')
                return EventsProcessTaskResult(
                    version=version,
                    network=network,
                    error='Nobody processing events',
                    lock=lock_name
                ).to_dict()

        return EventsProcessTaskResult(
            version=version,
            network=network,
            lock=lock_name
        ).to_dict()
    finally:
        try:
            if lock.owned():
                lock.release()
        except KeyError:
            pass
        except Exception as e:
            logger.exception(f'{self.name} {version} {network}: Cannot release lock! Lock: {lock_name}')
