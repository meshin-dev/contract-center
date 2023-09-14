import logging
from typing import Union

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger(__name__)

GENESIS_EVENT_NAME_DEFAULT = 'Initialized'


class Sync(models.Model):
    name = models.SlugField(
        _('Name'),
        max_length=255,
        unique=True,
        help_text=_('Unique sync name as a slug to safely use across all workers and as part of lock names')
    )
    enabled = models.BooleanField(
        _('Events Sync Enabled'),
        default=False,
        blank=True,
        help_text=_('Dynamically enable or disable this sync')
    )
    process_enabled = models.BooleanField(
        _('Events Processing Enabled'),
        default=False,
        blank=True,
        help_text=_('Dynamically enable or disable events processing')
    )

    # Data versioning so that it would be easy to switch between versions or sync next data while using current one
    sync_data_version = models.BigIntegerField(
        blank=True,
        null=True,
        help_text=_('Version of the data that is currently being synced. Dont change it manually')
    )
    active_data_version = models.BigIntegerField(
        blank=True,
        null=True,
        help_text=_('Version of the data that is currently active. Dont change it manually')
    )

    # This field is not going to be used in the model, only in the admin interface.
    create_new_version = models.BooleanField(
        default=False,
        help_text=_('Create new version of the data. If sync is active, sync will be working with new version')
    )
    clear_versions = models.BooleanField(
        default=False,
        help_text=_('Clear all versions of the data. If sync is active, it will be stopped')
    )
    auto_switch_to_synced_version = models.BooleanField(
        default=False,
        help_text=_('Automatically switch to synced version when sync is finished')
    )
    process_from_block = models.BigIntegerField(
        blank=True,
        null=True,
        help_text=_('Block from which the sync should process events. '
                    'If empty - it will automatically calculate first available block in events table. '
                    'During processing new events, this field will be updated with the last processed block')
    )
    process_block_range = models.IntegerField(
        _("Process Block Range"),
        default=10,
        blank=True,
        help_text=_('How many blocks should be processed per one task')
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
    contract_genesis_block = models.BigIntegerField(
        _("Contract Genesis Block Number"),
        default=0,
        blank=True,
        help_text=_("Block from which the sync should always start. If empty - "
                    f"it will automatically calculate genesis block from '{GENESIS_EVENT_NAME_DEFAULT}' event")
    )
    genesis_event_name = models.CharField(
        _("Genesis Event Name"),
        default=GENESIS_EVENT_NAME_DEFAULT,
        blank=True,
        help_text=_("Event name which will be used to calculate genesis block")
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
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

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
            logger.error(f'Can not find sync object with params: {args}, {kwargs}')
            return None

    def get_absolute_url(self) -> str:
        return reverse("sync:detail", kwargs={"pk": self.pk})

    def save(self, *args, **kwargs) -> None:
        if self.clear_versions:
            self.last_synced_block_number = 0
            self.sync_data_version = None
            self.active_data_version = None
            self.clear_versions = False
            self.enabled = False

        # Set sync_data_version and active_data_version to timezone.now if they are empty.
        elif self.sync_data_version is None and self.active_data_version is None:
            self.sync_data_version = int(timezone.now().timestamp())
            self.active_data_version = self.sync_data_version
            self.last_synced_block_number = 0

        # Update the data_version to timezone.now if the create_new_version checkbox is checked.
        elif self.create_new_version:
            self.sync_data_version = int(timezone.now().timestamp())
            self.last_synced_block_number = 0

        self.create_new_version = False
        super().save(*args, **kwargs)

    def switch_latest_data_version(self) -> None:
        if not self.is_latest_data_version() and self.auto_switch_to_synced_version:
            self.active_data_version = self.sync_data_version
            self.save()

    def is_latest_data_version(self) -> bool:
        return self.sync_data_version == self.active_data_version

    def __str__(self):
        return f'{self.name.replace("_", " ").title()}'
