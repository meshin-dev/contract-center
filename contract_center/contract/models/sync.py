from typing import Union

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _


# TODO: move this to sync model events list field
# class EventV4(Enum):
#     EVENT_OPERATOR_ADDED = 'OperatorAdded'
#     EVENT_OPERATOR_REMOVED = 'OperatorRemoved'
#     EVENT_OPERATOR_FEE_EXECUTION = 'OperatorFeeExecuted'
#     EVENT_OPERATOR_FEE_DECLARATION = 'OperatorFeeDeclared'
#     EVENT_ACCOUNT_LIQUIDATED = 'ClusterLiquidated'
#     EVENT_ACCOUNT_ENABLED = 'ClusterReactivated'
#     EVENT_VALIDATOR_ADDED = 'ValidatorAdded'
#     EVENT_VALIDATOR_REMOVED = 'ValidatorRemoved'
#     EVENT_CLUSTER_DEPOSITED = 'ClusterDeposited'
#     EVENT_CLUSTER_WITHDRAWN = 'ClusterWithdrawn'
#     EVENT_ACCOUNT_FEE_RECIPIENT_ADD = 'FeeRecipientAddressUpdated'
#
#
# # Use both upper and lower case to not care about it
# event_versions: Dict[str, Type[Enum]] = dict(
#     v4=EventV4,
#     V4=EventV4,
# )


class Sync(models.Model):
    """
    0x45B831727DC96035e6a2f77AAAcE4835195a54Af
    https://eth-goerli.g.alchemy.com/v2/rI4bIEGveSkw0KYAYO8VMIuMJA0QtNIA
    wss://eth-goerli.g.alchemy.com/v2/rI4bIEGveSkw0KYAYO8VMIuMJA0QtNIA
    """
    name = models.SlugField(
        _('Name'),
        max_length=255,
        unique=True,
        help_text=_('Unique sync name as a slug to safely use across all workers and as part of lock names')
    )
    enabled = models.BooleanField(
        _('Enabled'),
        default=False,
        blank=True,
        help_text=_('Dynamically enable or disable this sync')
    )
    contract_address = models.CharField(
        _("Contract Address"),
        max_length=255,
        help_text=_('ETH contract address')
    )
    contract_abi = models.JSONField(
        _("Contract ABI"),
        help_text=_('ETH Contract ABI'),
        default=dict
    )
    last_synced_block_number = models.BigIntegerField(
        _("Last Synced Block Number"),
        default=0,
        blank=True,
        help_text=_("During synchronization saving here the last synced block number")
    )
    sync_block_range = models.IntegerField(
        _("Block Range"),
        default=1000,
        blank=True,
        help_text='Set as 0 to always take till current block'
    )
    node_http_address = models.CharField(
        _("Node HTTP Address"),
        max_length=255,
        help_text=_('Used by Web3 HTTPProvider during ranged sync')
    )
    node_websocket_address = models.CharField(
        _("Node WebSocket Address"),
        max_length=255,
        help_text=_('Used by Web3 SocketProvider for listening live events')
    )
    live_events_connect_timeout = models.IntegerField(
        _("Live events connect timeout"),
        blank=True,
        default=60,
        help_text=_('How many seconds should wait for websocket connections')
    )
    live_events_read_timeout = models.IntegerField(
        _("Live events read timeout"),
        blank=True,
        default=60,
        help_text=_('How many seconds should wait for new data from opened websocket connections')
    )
    meta = models.JSONField(
        _("Meta data"),
        default=dict,
        help_text=_('Meta data specific for this sync. '
                    'Can be used by other modules to customize their behavior for this sync.'
                    'In periodic task specific sync can be used like this: '
                    '"{\"name\": \"My Sync Name"}", or using some meta data: "{\"meta__network\": \"goerli"}". '
                    'Under the hood it will be using Sync.objects.get(**kwargs)')
    )
    event_names = models.JSONField(
        _('Event names'),
        default=list,
        help_text=_('Array of event names that should be synced')
    )

    class Meta:
        pass

    @staticmethod
    def load(*args, **kwargs) -> Union[None, 'Sync']:
        """
        Find sync object safely
        :param args:
        :param kwargs:
        :return:
        """
        try:
            return Sync.objects.get(*args, **kwargs)
        except(KeyError, ValueError, ObjectDoesNotExist):
            return None

    def get_absolute_url(self) -> str:
        return reverse("sync:detail", kwargs={"pk": self.pk})
