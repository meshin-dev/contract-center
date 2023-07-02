from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.ssv_network.models.events import TestnetV4Event, MainnetV4Event, EventModel


@admin.register(TestnetV4Event)
class TestnetV4EventAdmin(ModelAdmin):
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
    list_display = ["event", "owner_address", "transaction_hash"]
    search_fields = ["event", "address", "transactionHash", "args"]
    ordering = ["blockNumber", "transactionIndex"]
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
        "createdAt",
        "updatedAt",
    )

    # TODO: extract this logic to reusable methods
    def block_number(self, obj: EventModel):
        links = {
            'goerli': 'https://goerli.etherscan.io/block/%s',
            'prater': 'https://goerli.etherscan.io/block/%s',
            'mainnet': 'https://etherscan.io/block/%s',
        }
        return format_html(
            '<a href="{}" target="_blank">{}</a>',
            links.get(str(obj.network)) % obj.blockNumber,
            obj.blockNumber
        )

    def owner_address(self, obj: EventModel):
        links = {
            'goerli': 'https://goerli.etherscan.io/address/%s',
            'prater': 'https://goerli.etherscan.io/address/%s',
            'mainnet': 'https://etherscan.io/address/%s',
        }
        return format_html(
            '<a href="{}" target="_blank">{}</a>',
            links.get(str(obj.network)) % obj.address,
            obj.address
        )

    def transaction_hash(self, obj: EventModel):
        links = {
            'goerli': 'https://goerli.etherscan.io/tx/%s',
            'prater': 'https://goerli.etherscan.io/tx/%s',
            'mainnet': 'https://etherscan.io/tx/%s',
        }
        hash = str(obj.transactionHash)
        if not hash.startswith('0x'):
            hash = '0x' + hash
        return format_html(
            '<a href="{}" target="_blank">{}</a>',
            links.get(str(obj.network)) % hash,
            hash
        )


@admin.register(MainnetV4Event)
class TestnetV4EventAdmin(TestnetV4EventAdmin):
    pass
