import json
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable

from celery import shared_task, group
from celery.result import AsyncResult
from dataclasses_json import dataclass_json
from django.core.serializers.json import DjangoJSONEncoder
from django.db.models.signals import Signal
from django.dispatch import receiver


@dataclass_json
@dataclass
class EventReceiverResult:
    """
    Class to represent a unified structure to return from signal handlers
    when syncing raw contract events.

    Usage example:

        EventReceiverResult(
            receiver='my_receiver',
            saved_total=5,
            saved_events={...}
        ).to_dict()
    """
    receiver: str
    saved_total: int = 0
    saved_events: dict = field(default_factory=dict)
