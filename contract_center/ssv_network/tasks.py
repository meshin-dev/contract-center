import logging
import os
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Type, List, Union

from dataclasses_json import dataclass_json
from django.db import transaction
from django.db.models import Q
from redis.lock import Lock

from config.celery_app import app
from contract_center.contract.models import Sync
from contract_center.contract.receivers import EventReceiverResult
from contract_center.contract.tasks import SmartTask
from contract_center.ssv_network.models.events import get_event_model, EventModel
from contract_center.ssv_network.signals import process_event_signal

logger = logging.getLogger(__name__)


class ProcessStatus(Enum):
    NEW = None
    PROCESSED = 'processed'


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


class EventsProcessTask(SmartTask):
    name = 'contract.events_process'
    queue = 'queue_events_process'

    sync: Sync = None
    version: str = None
    network: str = None

    def get_lock_name(self) -> str:
        """
        Get the lock name for the task
        :return:
        """
        return f'{self.name}_{self.version.lower()}_{self.network.lower()}'

    def lock_acquire(self) -> Union[None, EventsProcessTaskResult]:
        """
        Acquire the lock for the task
        :return:
        """
        lock_name = self.get_lock_name()
        lock: Lock = self.cache.lock(lock_name, timeout=self.lock_timeout_sec)
        if lock.locked():
            return EventsProcessTaskResult(
                version=self.version,
                network=self.network,
                lock=lock_name,
                reason='Processor is already locked'
            ).to_dict()
        if not lock.acquire(blocking_timeout=self.lock_blocking_timeout_sec):
            return EventsProcessTaskResult(
                version=self.version,
                network=self.network,
                lock=lock_name,
                reason=f'Could not acquire the lock during {self.lock_blocking_timeout_sec} seconds'
            ).to_dict()
        return None

    @staticmethod
    def fetch_events_callable(
        event_model: Type[EventModel],
        process_status: ProcessStatus = None,
        limit: int = 1  # TODO: make it configurable through Sync model
    ) -> List[EventModel]:
        """
        Get events to process
        :param event_model:
        :param process_status:
        :param limit:
        :return:
        """
        condition = dict(
            process_status__isnull=True,
        ) if process_status is None else dict(
            process_status=process_status.value,
        )
        return event_model.objects.filter(Q(**condition)).order_by('blockNumber', 'transactionIndex')[:limit]

    def run(self, *args, **kwargs):
        """
        Run the task
        :param args:
        :param kwargs:
        :return:
        """
        self.version = kwargs.get('version')
        assert self.version, 'Version is required'
        self.network = kwargs.get('network')
        assert self.network, 'Network is required'

        can_self_schedule = False

        try:
            # Try to acquire the lock for this task
            try:
                self.get_lock_manager().lock_acquire()
            except Exception as e:
                return EventsProcessTaskResult(
                    version=self.version,
                    network=self.network,
                    lock=self.get_lock_name(),
                    reason=str(e),
                ).to_dict()

            print(f'Acquired lock. PID: {os.getpid()} Thread: {threading.get_native_id()}')

            # Get sync info
            try:
                self.get_lock_manager().submit(lambda: Sync.load(
                    meta__version=self.version,
                    meta__network=self.network,
                    enabled=True,
                ))
                self.get_lock_manager().keep_alive(raise_exceptions=True)

                sync: Sync = self.get_lock_manager().result(clean=True).result()
                if not sync:
                    return EventsProcessTaskResult(
                        version=self.version,
                        network=self.network,
                        lock=self.get_lock_name(),
                        reason='Sync is not enabled or not created'
                    ).to_dict()
            except Exception as e:
                self.log('Could not get sync info', log_method=logger.error)
                self.log(e)
                return EventsProcessTaskResult(
                    version=self.version,
                    network=self.network,
                    lock=self.get_lock_name(),
                    reason=f'Could not get sync info',
                    error=str(e),
                ).to_dict()

            # Get proper model to get events
            event_model: Type[EventModel] = get_event_model(self.version, self.network)
            if not event_model:
                return EventsProcessTaskResult(
                    version=self.version,
                    network=self.network,
                    lock=self.get_lock_name(),
                    reason='Could not find event model'
                ).to_dict()

            # Get events to process in a thread
            try:
                self.get_lock_manager().submit(lambda: self.fetch_events_callable(event_model))
                self.get_lock_manager().keep_alive(raise_exceptions=True)
                events: List[EventModel] = self.get_lock_manager().result(clean=True).result()
                self.log(f'Fetched {len(events)} events to process')
            except Exception as e:
                self.log('Could not fetch events to process', log_method=logger.error)
                self.log(e)
                return EventsProcessTaskResult(
                    version=self.version,
                    network=self.network,
                    lock=self.get_lock_name(),
                    reason=f'Could not fetch events to process',
                    error=str(e),
                ).to_dict()

            # Iterate over events and process them
            for event in events:
                # Start django transaction
                with transaction.atomic():
                    # Trigger signal to process event
                    self.get_lock_manager().submit(lambda: process_event_signal.send(
                        sender=event_model,
                        version=self.version,
                        network=self.network,
                        event=event
                    ))
                    self.get_lock_manager().keep_alive(raise_exceptions=True)

                    # Collect receivers' results
                    results: List[EventReceiverResult] = [
                        result.result() for result in self.get_lock_manager().result(clean=True).result()
                    ]
                    if not len(results):
                        self.log('No results from receivers. Looks like no subscribers to process events',
                                 log_method=logger.warning)

                        return EventsProcessTaskResult(
                            version=self.version,
                            network=self.network,
                            error='Nobody processing events',
                            lock=self.get_lock_name()
                        ).to_dict()

                    # Update event status
                    event.process_status = ProcessStatus.PROCESSED.value
                    event.save(update_fields=['process_status'])

                    self.log(f'Processed event: "{event.event}". '
                             f'Block: {event.blockNumber}. '
                             f'Transaction Hash: {event.transactionHash}')

            # Return success
            can_self_schedule = len(events)
            return EventsProcessTaskResult(
                version=self.version,
                network=self.network,
                lock=self.get_lock_name()
            ).to_dict()

        except Exception as e:
            self.log('Can not process events')
            self.log(e)
            return EventsProcessTaskResult(
                version=self.version,
                network=self.network,
                lock=self.get_lock_name(),
                error=str(e)
            ).to_dict()
        finally:
            self.get_lock_manager().close()
            print(f'Released lock. PID: {os.getpid()} Thread: {threading.get_native_id()}')
            # Check if there is more events to process and trigger myself
            if self.get_events_to_process_count() > 0 and can_self_schedule:
                EventsProcessTask().apply_async(
                    args=args,
                    kwargs=kwargs,
                )

    def get_events_to_process_count(self) -> int:
        """
        Get count of events to process
        :return:
        """
        event_model: Type[EventModel] = get_event_model(self.version, self.network)
        if not event_model:
            return 0
        return event_model.objects.filter(
            process_status=ProcessStatus.NEW.value
        ).count()


app.register_task(EventsProcessTask)
