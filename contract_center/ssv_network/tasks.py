import logging
from dataclasses import dataclass
from enum import Enum
from typing import Type, List, Union

from dataclasses_json import dataclass_json
from django.db import transaction
from django.db.models import Q, Min
from redis.lock import Lock

from config.celery_app import app
from contract_center.contract.models import Sync
from contract_center.contract.receivers import EventReceiverResult
from contract_center.contract.tasks import SmartTask
from contract_center.ssv_network.models.events import get_event_model, EventModel
from contract_center.ssv_network.signals import event_process_signal

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
    sync: str = ''
    version: str = ''
    network: str = ''
    lock: str = ''
    reason: str = ''
    error: str = ''
    processed_events: int = 0


class EventsProcessTask(SmartTask):
    name = 'contract.events_process'
    queue = 'queue_events_process'

    sync: Sync = None

    def __init__(self):
        super().__init__()
        self.can_self_schedule = False

    def get_lock_name(self) -> str:
        """
        Get the lock name for the task
        :return:
        """
        return f'{self.name}.{self.sync.name}'

    def lock_acquire(self) -> Union[None, EventsProcessTaskResult]:
        """
        Acquire the lock for the task
        :return:
        """
        lock_name = self.get_lock_name()
        lock: Lock = self.cache.lock(lock_name, timeout=self.lock_timeout_sec)
        if lock.locked():
            return EventsProcessTaskResult(
                version=self.sync.meta.get('version'),
                network=self.sync.meta.get('network'),
                lock=lock_name,
                reason='Processor is already locked'
            ).to_dict()
        if not lock.acquire(blocking_timeout=self.lock_blocking_timeout_sec):
            return EventsProcessTaskResult(
                version=self.sync.meta.get('version'),
                network=self.sync.meta.get('network'),
                lock=lock_name,
                reason=f'Could not acquire the lock during {self.lock_blocking_timeout_sec} seconds'
            ).to_dict()
        return None

    def run(self, *args, **kwargs):
        """
        Run the task
        :param args:
        :param kwargs:
        :return:
        """
        sync_name = kwargs.get('name')
        assert sync_name, 'Sync name is required'

        task_result = EventsProcessTaskResult(
            sync=sync_name,
        )

        try:
            # Get sync info
            try:
                self.sync = Sync.load(
                    name=sync_name,
                    enabled=True,
                )
                if not self.sync:
                    task_result.reason = 'Sync is not enabled or not created'
                    return task_result.to_dict()
                task_result.version = self.sync.meta.get('version')
                task_result.network = self.sync.meta.get('network')
            except Exception as e:
                logger.error('Could not get sync info')
                logger.exception(e)
                task_result.reason = 'Could not get sync info'
                task_result.error = str(e)
                return task_result.to_dict()

            if not self.sync.process_enabled:
                task_result.reason = 'Processing events is not enabled'
                return task_result.to_dict()

            # Try to acquire the lock for this task
            try:
                self.get_lock_manager().lock_acquire()
                task_result.lock = self.get_lock_name()
            except Exception as e:
                task_result.reason = 'Could not acquire the lock'
                task_result.error = str(e)
                return task_result.to_dict()

            # Get events to process in a thread
            try:
                self.get_lock_manager().submit(self.get_events_for_processing)
                events: List[EventModel] = self.get_lock_manager().result()
                logger.info(f'Fetched {len(events)} events to process')
            except Exception as e:
                logger.error('Could not fetch events to process')
                logger.exception(e)
                task_result.reason = 'Could not fetch events to process'
                task_result.error = str(e)
                return task_result.to_dict()

            # Iterate over events and process them
            def process_events(events_list: List[EventModel]):
                for event in events_list:
                    # Start django transaction
                    with transaction.atomic():
                        # Trigger signal to process event
                        results: List[EventReceiverResult] = event_process_signal.send(
                            sender=self.__class__,
                            instance=self,
                            event=event
                        )
                        if not results:
                            logger.warning('No results from receivers: subscribers to process events')
                            task_result.reason = f'Nobody processing event: {event.event}'
                            return task_result.to_dict()

                        # Update process status
                        saved_total = sum([r[1].saved_total for r in results]) if len(results) else 0
                        if saved_total:
                            event.process_status = ProcessStatus.PROCESSED.value
                            event.save(update_fields=['process_status'])

                        # Update sync info with last processed block
                        self.sync.process_from_block = int(event.blockNumber) + 1
                        self.sync.save(update_fields=['process_from_block'])

                        logger.debug(f'Processed event: "{event.event}". '
                                     f'Block: {event.blockNumber}. '
                                     f'Transaction Hash: {event.transactionHash}. '
                                     f'Transaction Index: {event.transactionIndex}. '
                                     f'Log Index: {event.logIndex}. '
                                     f'Contract: {event.address}. '
                                     f'Saved total: {saved_total}.')

            # Process events in a thread
            self.get_lock_manager().submit(lambda: process_events(events))
            self.get_lock_manager().result()

            # Return success
            self.can_self_schedule = len(events) > 0
            task_result.processed_events = len(events)
            task_result.reason = 'Processed events'
            return task_result.to_dict()
        except Exception as e:
            logger.error('Can not process events')
            logger.exception(e)
            task_result.reason = 'Can not process events'
            task_result.error = str(e)
            return task_result.to_dict()
        finally:
            self.post_process()

    def post_process(self):
        if not self.manager:
            return
        if self.get_lock_manager().lock_owned():
            # Check if possible to trigger self to immediately process next events
            if self.can_self_schedule:
                logger.debug(f'Self scheduling for next events processing: {self.sync.name}')
                self.get_lock_manager().submit(lambda: self.get_events_for_processing(count=True))
                events_to_process_count = self.get_lock_manager().result()
                if events_to_process_count > 0:
                    EventsProcessTask().apply_async(
                        kwargs=dict(
                            name=self.sync.name,
                            countdown=1,
                        )
                    )

            self.get_lock_manager().submit(self.sync.switch_latest_data_version)
            self.get_lock_manager().result()
        else:
            self.sync.switch_latest_data_version()
        self.get_lock_manager().close()

    def get_event_model(self) -> Type[EventModel]:
        # Get a proper model to get events
        try:
            event_model: Type[EventModel] = get_event_model(self.sync.meta.get('network'))
            if not event_model:
                raise Exception(f'Could not get event model for network: {self.sync.meta.get("network")}')
            return event_model
        except Exception as e:
            logger.error('Could not get event model. '
                         'Sync is not enabled or process is not enabled, or sync does not exist')
            raise Exception('Could not get events model')

    def get_block_to_process_from(self) -> int:
        return self.sync.process_from_block \
            or get_event_model(
                self.sync.meta.get('network')
            ).objects.all().aggregate(
                Min('blockNumber')
            ).get('blockNumber__min')

    def get_events_for_processing(
        self,
        process_status: ProcessStatus = None,
        count: bool = False,
        limit: int = 10
    ) -> Union[List[EventModel], int]:
        """
        Get events to process
        :param count:
        :param process_status:
        :param limit:
        :return:
        """
        condition = dict(
            version=self.sync.meta.get('version'),
            network=self.sync.meta.get('network'),
            data_version=self.sync.sync_data_version,
            blockNumber__gte=self.get_block_to_process_from()
        )
        if process_status is None:
            condition['process_status__isnull'] = True
        else:
            condition['process_status'] = process_status.value
        result = self.get_event_model().objects.filter(
            Q(**condition)
        ).order_by(
            'blockNumber',
            'transactionIndex'
        )
        if count:
            return result.count()
        return result[:(self.sync.process_block_range or limit)]


app.register_task(EventsProcessTask)
