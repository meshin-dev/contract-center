import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from typing import List, Union, Dict, Callable

from celery import Task
from dataclasses_json import dataclass_json
from django.core.cache import caches
from django_redis.client import DefaultClient
from redis.lock import Lock

from config.celery_app import app
from contract_center.contract.models.sync import Sync
from contract_center.contract.receivers import EventReceiverResult
from contract_center.contract.signals import contract_fetched_events_signal
from contract_center.contract.web3.contract import Web3Contract

logger = logging.getLogger(__name__)


@dataclass_json
@dataclass
class EventsFetchTaskResult:
    """
    Dataclass representing the result of the EventsFetchTask.
    """
    task: str
    lock: str
    error: str = ''
    reason: str = ''
    block_to: int = 0
    block_from: int = 0
    total_events: int = 0
    context: dict = field(default_factory=dict)
    results: list = field(default_factory=list)


class SmartTask(Task):
    shared = True

    # How long to wait for a task to complete if it was not reacquired
    lock_ttl_sec = 15

    # How long to wait trying to acquire a lock before giving up
    lock_blocking_timeout_sec = 5

    # Period in seconds to reacquire a lock on a task if its long-lasting
    process_reacquire_each_sec = 5

    # Default cache client is RedisCacheClient
    cache: DefaultClient = caches['default']

    # Lock instance
    lock: Lock = None

    # Some context specific to particular task
    context: Dict = {}

    def get_lock(self) -> Lock:
        """
        Get the lock instance for the given task
        :return:
        """
        self.lock = self.lock or self.cache.lock(self.get_lock_name(), timeout=self.lock_ttl_sec)
        return self.lock

    def get_lock_name(self) -> str:
        return f'{self.name}.{self.__class__.__name__}'

    def lock_acquire(self) -> Union[None, EventsFetchTaskResult]:
        """
        Tries to acquire a lock for the task. If the lock cannot be acquired within a specified timeout,
        it returns a task result with a reason for failure. If the lock is successfully acquired,
        a debug level log message is written and None is returned.

        :return: A dictionary with task result information if the lock could not be acquired; otherwise, None.
        """
        if self.get_lock().locked():
            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                reason=f'Lock is already acquired'
            )
        if not self.get_lock().acquire(blocking_timeout=self.lock_blocking_timeout_sec):
            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                reason=f'Could not acquire lock after {self.lock_blocking_timeout_sec} seconds'
            )
        self.log('Acquired the lock', log_method=logger.debug)
        return None

    def lock_reacquire_loop(self, future: Future):
        """
        This method is used to continuously reacquire a lock during a long-lasting synchronization operation.

        The method operates in a loop, checking if the given Future is done. If it is, the loop breaks,
        if not, it attempts to reacquire the lock and logs the operation. The loop will sleep for a specified
        duration (self.process_reacquire_each_sec) before attempting the reacquisition again.

        Parameters:
        future (concurrent.futures.Future): Future object that represents a potentially long-lasting operation.
                                            The loop continues until this Future is done.

        Raises:
        Exception: Any exception that is raised during the lock reacquisition is propagated.

        Note: This method should be used when running a potentially long-lasting operation that requires a lock,
        but also needs to periodically reacquire the lock to not lose it and not start the same process in parallel.
        """
        while True:
            if future.done():
                break
            try:
                self.get_lock().reacquire()
                self.log(f'Reacquired lock during long lasting synchronization...', log_method=logging.debug)
            except Exception as e:
                raise
            time.sleep(self.process_reacquire_each_sec)

    def log(self, message, log_method: Callable = logger.info):
        if log_method == logger.exception or isinstance(message, BaseException):
            logger.exception(message)
        else:
            log_method(f'[ Task {self.name} ]: '
                       f'{message}. '
                       f'Context: {self.context}. '
                       f'Lock: {self.get_lock_name()}')


class EventsFetchTask(SmartTask):
    name = 'contract.events_fetch'
    queue = 'queue_events_fetch'

    sync: Sync = None
    web3_contract: Web3Contract = None
    block_from_back_offset = 2
    minimum_blocks_to_fetch = 10

    @property
    def contract(self) -> Web3Contract:
        """
        Get the contract instance associated with Sync
        :return:
        """
        self.web3_contract = self.web3_contract or Web3Contract(
            node_http_address=self.sync.node_http_address,
            node_websocket_address=self.sync.node_websocket_address,
            contract_address=self.sync.contract_address,
            contract_abi=self.sync.contract_abi,
        )
        return self.web3_contract

    def get_sync(self, **kwargs) -> Union[Sync, EventsFetchTaskResult]:
        """
        Tries to load sync information based on the provided keyword arguments. If the Sync is not found or not enabled,
        it returns a task result indicating the failure.

        :param kwargs: Keyword arguments used to load the Sync.

        :return: A Sync instance if it is found and enabled, else an instance of EventsFetchTaskResult containing
                 the task failure details.
        """
        sync = Sync.load(**kwargs)
        if not sync:
            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                reason=f'Sync is not enabled or does not exist: {kwargs}'
            )
        return sync

    def run(self, *args, **kwargs):
        """
        This is the main execution method of the EventsFetchTask class. It is responsible for fetching events from an Ethereum
        contract. The method follows these steps:

        1. Tries to acquire a lock to ensure no other instance is currently fetching events.
        2. Retrieves the sync information and checks its validity.
        3. Determines the block range to fetch events from.
        4. Fetches events from the Ethereum contract within the block range determined in step 3.
        5. If events are fetched, it sends a signal to receivers with the fetched events.
        6. Processes any errors occurred during the above steps, logs them, and returns corresponding results.
        7. In the end, releases the lock and if there are more blocks to fetch, it triggers the same task again asynchronously.
        """
        events = []
        block_from = block_to = block_last = None
        can_self_call = False
        try:
            # Try to acquire the lock for this task
            lock_result = self.lock_acquire()
            if lock_result:
                return lock_result.to_dict()

            # Get sync entry from kwargs
            self.sync = self.get_sync(**kwargs)
            if isinstance(self.sync, EventsFetchTaskResult):
                return self.sync.to_dict()

            self.log(f'Found sync for kwargs: {kwargs}', log_method=logger.debug)
            self.get_lock().reacquire()

            # Prepare block range to fetch
            block_from = int(self.sync.last_synced_block_number
                             if self.sync.last_synced_block_number >= self.sync.last_synced_block_number
                             else self.contract.get_genesis_block_number())
            block_last = int(self.contract.web3_http.eth.get_block('latest').get('number'))
            blocks_range = max(self.minimum_blocks_to_fetch, self.sync.sync_block_range)
            block_to = min(block_last, block_from + blocks_range)
            block_to = max(block_to, block_from)

            self.log(
                f'Found block range to sync events: {block_from}-{block_to}. Current block: {block_last}',
                log_method=logger.debug
            )
            self.get_lock().reacquire()

            # Fetch events
            fetch_params = dict(
                events=self.sync.event_names,
                block_from=block_from,
                block_to=block_to,
            )
            executor = ThreadPoolExecutor()
            fetch_events_future = executor.submit(lambda: self.contract.events_fetch(**fetch_params))

            # Reacquire the lock in parallel with fetching events
            self.lock_reacquire_loop(future=fetch_events_future)

            # Check for errors
            error = fetch_events_future.exception()
            if error:
                self.log(error)
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Could not fetch new events',
                    error=str(error),
                    block_from=block_from,
                    block_to=block_to,
                ).to_dict()

            events: List[Dict] = fetch_events_future.result()
            self.log(f'Fetched {len(events)} new events', log_method=logger.debug)
            self.get_lock().reacquire()

            if not len(events):
                self.sync.last_synced_block_number = block_to - self.block_from_back_offset
                self.sync.save()
                can_self_call = True
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    block_from=block_from,
                    block_to=block_to,
                ).to_dict()

            self.get_lock().reacquire()

            # Trigger signal to sync new events
            executor = ThreadPoolExecutor()
            new_events_signal_future = executor.submit(lambda: contract_fetched_events_signal.send(
                sender=self.__name__,
                instance=self,
                params=fetch_params,
                events=events,
            ))

            # Reacquire the lock in parallel with signal handlers work
            self.lock_reacquire_loop(future=new_events_signal_future)

            # Check out error from signal receivers and return error in case of failure
            error = new_events_signal_future.exception()
            if error:
                # Return error from task
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Could not send new events signal to receivers',
                    error=str(error),
                    block_from=block_from,
                    block_to=block_to,
                ).to_dict()

            # Collect receivers results
            results: List[EventReceiverResult] = new_events_signal_future.result()

            can_self_call = True

            if not len(results):
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Nobody is listening for new events',
                    block_from=block_from,
                    block_to=block_to,
                ).to_dict()

            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                block_from=block_from,
                block_to=block_to,
                total_events=len(events),
                results=[result[1].to_dict() for result in results]
            ).to_dict()

        except Exception as e:
            self.log('Can not fetch events')
            self.log(e)
            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                block_from=block_from,
                block_to=block_to,
                total_events=len(events),
            ).to_dict()
        finally:
            try:
                if self.get_lock().locked() and self.get_lock().owned():
                    self.get_lock().release()
            # except AttributeError:
            #     pass
            except Exception as e:
                self.log('Can not release lock')
                self.log(e)
            finally:
                if block_last and block_to and block_last - block_to > self.minimum_blocks_to_fetch and can_self_call:
                    EventsFetchTask().apply_async(args=args, kwargs=kwargs)


app.register_task(EventsFetchTask)
