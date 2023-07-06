import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, List, Union, Dict, Any

from redis import Redis
from redis.lock import Lock

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class LockManager:
    lock: Lock = None
    redis_client: Redis = None
    executor: Dict[str, ThreadPoolExecutor] = dict()

    def __init__(
        self,
        redis_client: Redis = None,
        lock: Union[Lock, str] = None,
        lock_timeout_sec: float = 5,
        lock_extend_from_sec: float = 2,
        lock_blocking_timeout_sec: float = 5,
        wait_for_futures_timeout_sec: float = 0.1,
    ):
        self.name = None

        # Redis instance
        self.redis_client = redis_client

        # Lock options
        self.lock_timeout = lock_timeout_sec
        self.lock_extend_from_sec = lock_extend_from_sec
        self.lock_blocking_timeout_sec = lock_blocking_timeout_sec
        self.wait_for_futures_timeout_sec = wait_for_futures_timeout_sec

        # Futures
        self.future_options = {}

        # Lock instance
        self.set_lock(lock=lock)

    @staticmethod
    def set_redis_client(redis_client: Redis):
        """
        Set the redis client as a class variable so all instances can use it

        :param redis_client: An instance of redis.Redis
        """
        LockManager.redis_client = redis_client

    def set_lock(self, lock: Lock):
        """
        Set the lock instance for this LockManager instance
        :param lock:
        :return:
        """
        if isinstance(lock, Lock):
            self.name = getattr(lock, 'name', lock.__class__.__name__)
            self.set_lock(lock)
        elif isinstance(lock, str):
            self.name = lock

    def submit(self, fn: Callable, executor_name: str = None, *args, **kwargs) -> Future:
        """
        Submit a function to the executor and return the future.

        :param fn: The function to submit to the executor.
        :param executor_name: The name of executor to use. If not specified, the default executor is used.
        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function. Special keyword arguments 'race_if_first'
        is used to control the behavior of the LockManager.
        :return: The future representing the execution of the function.
        """
        # Get executor instance
        executor_name = executor_name or self.name
        executor: ThreadPoolExecutor = self.get_executor(name=executor_name)

        # Submit the function
        race_if_first = kwargs.pop('race_if_first', False)
        future = executor.submit(fn, *args, **kwargs)

        # Save future options
        self.future_options[future] = dict(
            executor_name=executor_name,
            future_kwargs=kwargs,
            race_if_first=race_if_first,
        )
        return future

    def get_lock(self) -> Lock:
        """
        Get the lock instance for the given task
        :return:
        """
        if not self.lock:
            if not LockManager.redis_client:
                raise EnvironmentError('Redis client is not set')
            self.lock = LockManager.redis_client.lock(self.name, timeout=self.lock_timeout)
        return self.lock

    def lock_owned(self) -> bool:
        """
        Returns True if the lock is owned by the current thread, False otherwise
        :return:
        """
        lock: Lock = self.get_lock()
        return lock \
               and lock.locked() \
               and hasattr(lock, 'local') \
               and hasattr(lock.local, 'token') \
               and lock.owned()

    def lock_acquire(self):
        """
        Tries to acquire a lock for the task. If the lock cannot be acquired within a specified timeout,
        it returns a task result with a reason for failure. If the lock is successfully acquired,
        a debug level log message is written and None is returned.

        :return: A dictionary with task result information if the lock could not be acquired; otherwise, None.
        """
        if self.get_lock().locked() and not self.lock_owned():
            raise EnvironmentError(f'Lock is already acquired for {self.name} from other process')

        if not self.get_lock().acquire(blocking=True, blocking_timeout=self.lock_blocking_timeout_sec):
            raise EnvironmentError(f'Failed to acquire lock for {self.name} after {self.lock_blocking_timeout_sec} seconds')
        logger.debug(f'Acquired the lock for {self.name}')

    def wait_for_futures(self, raise_exceptions: bool = True):
        """
        The main method that keeps the lock alive until all futures are done or an exception is raised.
        It sleeps for a predefined time (self.wait_for_futures_timeout_sec) between checks for the futures' states.
        If the time passed since the start of the loop is more than self.lock_extend_from_sec, it extends the lock.

        :param raise_exceptions: A flag that indicates whether to raise exceptions that occurred in the futures.
        """
        # Check if lock was not locked before and do it
        if not self.get_lock().locked():
            self.lock_acquire()

        # Start the loop of extending the lock time while other futures are not done
        start = time.time()
        while any(not future.done() for future in self.future_options):
            # Check if any of the futures is done and have exception which should be raised forward
            for future in self.future_options:
                if future.done():
                    if self.future_options[future]['race_if_first']:
                        return
                    if raise_exceptions and future.exception():
                        raise future.exception()

            # Check if it should extend the lock
            time_passed = float(time.time() - start)
            if time_passed > self.lock_extend_from_sec and self.lock_owned():
                start = time.time()
                self.get_lock().extend(time_passed)
            time.sleep(self.wait_for_futures_timeout_sec)

    def results(self, clean: bool = True, raise_exceptions: bool = True) -> List[Any]:
        self.wait_for_futures(raise_exceptions=raise_exceptions)
        return [future.result() for future in self.future_results(clean=clean)]

    def future_results(self, clean: bool = True, raise_exceptions: bool = True) -> List[Future]:
        self.wait_for_futures(raise_exceptions=raise_exceptions)
        results = [future for future in self.future_options]
        if clean:
            self.shutdown_executors()
            self.future_options = {}
        return results

    def result(self, clean: bool = True, raise_exceptions: bool = True) -> Any:
        self.wait_for_futures(raise_exceptions=raise_exceptions)
        return self.future_result(clean=clean).result()

    def future_result(self, clean: bool = True, raise_exceptions: bool = True) -> Future:
        self.wait_for_futures(raise_exceptions=raise_exceptions)
        result = list(self.future_options.keys())[-1]
        if clean:
            self.shutdown_executors()
            self.future_options = {}
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        """
        Exit the context manager, releasing the lock and shutting down the executor.
        """
        logger.debug(f'Closing lock manager: {self.name}')
        self.shutdown_executors()
        self.lock_release()
        logger.debug(f'Closed lock manager: {self.name}')

    def lock_release(self):
        """
        Releases the lock if it is owned by the current process.
        :return:
        """
        try:
            if self.lock_owned():
                self.get_lock().release()
                logger.debug(f'Lock has been released: {self.name}')
        except Exception as e:
            logger.error(f'Can not release lock: {self.name}')
            logger.exception(e)

    def get_executor(self, name: str = None, *args, **kwargs) -> ThreadPoolExecutor:
        """
        Get the executor instance for the given task
        :return:
        """
        thread_name_prefix = name or self.name
        if not self.executor.get(thread_name_prefix):
            self.executor[thread_name_prefix] = ThreadPoolExecutor(
                thread_name_prefix=thread_name_prefix,
                *args,
                **kwargs,
            )
        return self.executor.get(thread_name_prefix)

    def shutdown_executors(self, name: str = None, wait=False, cancel_futures=False):
        """
        Shutdown the executor if it is not shutdown yet
        :param name:
        :param wait:
        :param cancel_futures:
        :return:
        """
        for executor_name in self.executor:
            try:
                if name and executor_name != name:
                    continue
                self.executor.get(executor_name).shutdown(wait=wait, cancel_futures=cancel_futures)
            except:
                pass

        if name and name in self.executor:
            del self.executor[name]
        else:
            self.executor = {}
