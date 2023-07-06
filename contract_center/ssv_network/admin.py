from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.library.helpers.links import get_etherscan_link
from contract_center.ssv_network.models.events import TestnetEvent, MainnetEvent, EventModel


@admin.register(TestnetEvent)
class TestnetEventAdmin(ModelAdmin):
    fieldsets = (
        (
            _("Data"), {
                "fields": (
                    "version",
                    "network",
                    "event",
                    "owner_address",
                    "block_number",
                    "logIndex",
                    "transactionIndex",
                    "transaction_hash",
                    "blockHash",
                    "args",
                )
            }
        ),
        (
            _("Processing"), {
                "fields": (
                    "data_version",
                    "process_status",
                    "errors",
                )
            }
        ),
        (
            _("Date & Time"), {
                "fields": (
                    "createdAt",
                    "updatedAt",
                )
            }
        ),
    )
    list_display = ["network", "version", "event", "owner_address", "transaction_hash", "block_number", "transactionIndex"]
    search_fields = ["event", "address", "transactionHash", "args"]
    ordering = ["-blockNumber", "-transactionIndex"]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }
    readonly_fields = (
        "version",
        "network",
        "event",
        "owner_address",
        "block_number",
        "logIndex",
        "transactionIndex",
        "transaction_hash",
        "blockHash",
        "data_version",
        "createdAt",
        "updatedAt",
    )
    list_filter = ('network', 'version', 'data_version', 'process_status', 'event',)

    def block_number(self, obj: EventModel):
        return format_html(
            '<a href="{}" target="_blank">{}</a>',
            get_etherscan_link(str(obj.network), 'block/%s') % obj.blockNumber,
            obj.blockNumber
        )

    def owner_address(self, obj: EventModel):
        return format_html(
            '<a href="{}" target="_blank">{}</a>',
            get_etherscan_link(str(obj.network), 'address/%s') % obj.address,
            obj.address
        )

    def transaction_hash(self, obj: EventModel):
        hash = str(obj.transactionHash)
        if not hash.startswith('0x'):
            hash = '0x' + hash
        return format_html(
            '<a href="{}" target="_blank">{}</a>',
            get_etherscan_link(str(obj.network), 'tx/%s') % hash,
            hash
        )


@admin.register(MainnetEvent)
class TestnetEventAdmin(TestnetEventAdmin):
    pass
