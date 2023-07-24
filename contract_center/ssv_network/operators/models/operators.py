from enum import Enum
from typing import Type

from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from contract_center.ssv_network.models.events import TestnetEvent, MainnetEvent


class OperatorModel(models.Model):
    """
    Base operator model
    """
    operator_id = models.IntegerField(_("Operator ID"))
    public_key = models.TextField(_("Operator Public Key"))
    owner = models.CharField(_("Owner Address"), max_length=255)
    block_number = models.BigIntegerField(_("Block Number"))

    fee = models.CharField(_("Fee"), max_length=255)
    previous_fee = models.CharField(_("Previous Fee"), max_length=255)
    declared_fee = models.CharField(_("Declared Fee"), max_length=255)

    is_valid = models.BooleanField(_("Is Valid"), default=True)
    is_deleted = models.BooleanField(_("Is Deleted"), default=False)

    errors = models.JSONField(_("Errors"), default=list, blank=True)

    version = models.CharField(_("Version"), max_length=255)
    network = models.CharField(_("Network"), max_length=255)
    data_version = models.BigIntegerField(_("Data Version"))

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
        unique_together = ['operator_id', 'network', 'version', 'data_version']

    class Sync:
        class Events(Enum):
            OperatorAdded = 'OperatorAdded'
            OperatorRemoved = 'OperatorRemoved'
            OperatorFeeExecuted = 'OperatorFeeExecuted'
            OperatorFeeDeclared = 'OperatorFeeDeclared'

    def get_absolute_url(self) -> str:
        return reverse("operator:detail", kwargs={"operator_id": self.operator_id})

    def __str__(self):
        return f"<Operator version={self.version} network={self.network} id={self.operator_id} />"


class TestnetOperator(OperatorModel):
    events = models.ManyToManyField(TestnetEvent, blank=True)


class MainnetOperator(OperatorModel):
    events = models.ManyToManyField(MainnetEvent, blank=True)


def get_operator_model(network: str) -> Type[OperatorModel]:
    """
    Get operator for a given version and network
    :param network:
    :return:
    """
    if network.lower().strip() == 'mainnet':
        return MainnetOperator
    return TestnetOperator
