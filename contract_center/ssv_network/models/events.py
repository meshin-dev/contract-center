from typing import Type

from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _


class EventModel(models.Model):
    event = models.CharField(_("Event name"), max_length=255)
    logIndex = models.IntegerField(_("Log Index"))
    transactionIndex = models.IntegerField(_("Transaction Index"))
    transactionHash = models.CharField(_("Transaction Hash"), max_length=255)
    address = models.CharField(_("Address"), max_length=255)
    blockHash = models.CharField(_("Block Hash"), max_length=255)
    blockNumber = models.CharField(_("Block Number"), max_length=255)
    args = models.JSONField(_("Arguments"))

    process_status = models.CharField(_("Process Status"), max_length=255, default=None, blank=True, null=True)
    errors = models.JSONField(_("Errors"), default=list, blank=True)

    version = models.CharField(_("Version"), max_length=255)
    network = models.CharField(_("Network"), max_length=255)
    data_version = models.BigIntegerField(_("Data Version"))

    createdAt = models.DateTimeField(auto_now_add=True)
    updatedAt = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
        unique_together = ("transactionHash", "network", "version", "data_version",)

    def get_absolute_url(self) -> str:
        return reverse("event:detail", kwargs={"pk": self.transactionHash})


class TestnetEvent(EventModel):
    pass


class MainnetEvent(EventModel):
    pass


def get_event_model(network: str) -> Type[EventModel]:
    """
    Returns an event model for a given version and network
    :param network:
    :return:
    """
    if network.lower().strip() == 'mainnet':
        return MainnetEvent
    return TestnetEvent
