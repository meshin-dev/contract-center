import logging
from dataclasses import dataclass, field

from celery import Task
from dataclasses_json import dataclass_json
from django.core.cache import caches

from contract_center.library.locks import LockManager

logger = logging.getLogger(__name__)


@dataclass_json
@dataclass
class TaskResult:
    task: str
    lock: str
    error: str = ''
    reason: str = ''
    context: dict = field(default_factory=dict)
    results: list = field(default_factory=list)


class SmartTask(Task):
    name = 'default'

    shared = True

    # How long to wait for a task to complete if it was not reacquired
    lock_timeout_sec = 5

    # How long to wait trying to acquire a lock before giving up
    lock_blocking_timeout_sec = 1

    # Period in seconds to reacquire a lock on a task if its long-lasting
    lock_extend_from_sec = 2

    def __init__(self):
        super().__init__()
        self.manager = None
        self.context: dict = {
            'type': 'periodic'
        }

    def get_lock_manager(self) -> LockManager:
        """
        Building lock manager instance based on task preferences
        :return:
        """
        if not self.manager:
            self.manager = LockManager(
                lock=self.get_lock_name(),
                lock_timeout_sec=self.lock_timeout_sec,
                lock_extend_from_sec=self.lock_extend_from_sec,
                lock_blocking_timeout_sec=self.lock_blocking_timeout_sec,
            )
            self.manager.set_redis_client(caches['default'])
        return self.manager

    def get_lock_name(self) -> str:
        return f'{self.name}'
