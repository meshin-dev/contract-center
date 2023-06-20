from typing import Type

from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _


class OperatorModel(models.Model):
    """
    Base operator model
    """
    operatorId = models.IntegerField(_("Operator ID"))
    publicKey = models.TextField(_("Operator Public Key"))
    ownerAddress = models.CharField(_("Owner Address"), max_length=255)

    blockNumber = models.CharField(_("Block Number"), max_length=255)
    transactionIndex = models.IntegerField(_("Transaction Index"))

    fee = models.CharField(_("Fee"), max_length=255)
    previousFee = models.CharField(_("Previous Fee"), max_length=255)
    declaredFee = models.CharField(_("Declared Fee"), max_length=255)

    isValid = models.BooleanField(_("Is Valid"), default=True)
    isDeleted = models.BooleanField(_("Is Deleted"), default=False)

    errors = models.JSONField(_("Errors"), default=list, blank=True)

    version = models.CharField(_("Version"), max_length=255)
    network = models.CharField(_("Network"), max_length=255)

    createdAt = models.DateTimeField(auto_now_add=True)
    updatedAt = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

    def get_absolute_url(self) -> str:
        return reverse("operator:detail", kwargs={"operatorId": self.operatorId})


class TestnetV4Operator(OperatorModel):
    pass


class MainnetV4Operator(OperatorModel):
    pass


def get_operator_model(version: str, network: str) -> Type[OperatorModel]:
    """
    Get operator for a given version and network
    :param version:
    :param network:
    :return:
    """
    return dict(
        v4_prater=TestnetV4Operator,
        v4_mainnet=MainnetV4Operator,
    ).get(f'{version.lower()}_{network.lower()}')
