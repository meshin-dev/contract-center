from typing import Dict, Type

from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _


class EventModel(models.Model):
    version = models.CharField(_("Version"), max_length=255)
    network = models.CharField(_("Network"), max_length=255)
    event = models.CharField(_("Event name"), max_length=255)
    logIndex = models.IntegerField(_("Log Index"))
    transactionIndex = models.IntegerField(_("Transaction Index"))
    transactionHash = models.CharField(_("Transaction Hash"), max_length=255, unique=True, primary_key=True)
    address = models.CharField(_("Address"), max_length=255)
    blockHash = models.CharField(_("Block Hash"), max_length=255)
    blockNumber = models.CharField(_("Block Number"), max_length=255)
    args = models.JSONField(_("Arguments"))
    process_status = models.CharField(_("Process Status"), max_length=255, default=None, blank=True, null=True)
    errors = models.JSONField(_("Errors"), default=list, blank=True)

    class Meta:
        abstract = True

    def get_absolute_url(self) -> str:
        return reverse("event:detail", kwargs={"pk": self.transactionHash})


class TestnetV4Event(EventModel):
    pass


class MainnetV4Event(EventModel):
    pass


# All the upper and lower case to not care about them
event_models: Dict[str, Type[EventModel]] = dict(
    v4_prater=TestnetV4Event,
    V4_PRATER=TestnetV4Event,
    v4_mainnet=MainnetV4Event,
    V4_MAINNET=MainnetV4Event,
)
