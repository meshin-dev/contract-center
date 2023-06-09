from typing import Dict, Type

from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from contract_center.contract.models.sync import Sync
from contract_center.ssv_network.models.events import EventModel


class OperatorModel(models.Model):
    version = models.CharField(_("Version"), max_length=255)
    network = models.CharField(_("Network"), max_length=255)
    operatorId = models.IntegerField(_("Operator ID"))
    transactionIndex = models.IntegerField(_("Transaction Index"))
    ownerAddress = models.CharField(_("Owner Address"), max_length=255)
    blockNumber = models.CharField(_("Block Number"), max_length=255)
    errors = models.JSONField(_("Errors"), default=list, blank=True)

    class Meta:
        abstract = True

    def get_absolute_url(self) -> str:
        return reverse("event:detail", kwargs={"pk": self.pk})

    @staticmethod
    def process_event(sync: Sync, event: EventModel):
        """
        Initial functionality
        :param sync:
        :param event:
        :return:
        """
        # TODO: handle operator
        print(f'Processed operator related event: {event.event}')
        return 'success'


class TestnetV4Operator(OperatorModel):
    pass


class MainnetV4Operator(OperatorModel):
    pass


# All the upper and lower case to not care about them
operator_models: Dict[str, Type[OperatorModel]] = dict(
    v4_prater=TestnetV4Operator,
    V4_PRATER=TestnetV4Operator,
    v4_mainnet=MainnetV4Operator,
    V4_MAINNET=MainnetV4Operator,
)
