from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.library.helpers.links import get_etherscan_link
from contract_center.ssv_network.operators.models import TestnetOperator, MainnetOperator
from contract_center.ssv_network.operators.models.operators import OperatorModel

dont_break_style = 'white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'


@admin.register(TestnetOperator)
class TestnetOperatorAdmin(ModelAdmin):
    fieldsets = (
        (
            _("Operator"), {
                "fields": (
                    "operator_id",
                    "public_key",
                    "owner_address",
                )
            }
        ),
        (
            _("Fee"), {
                "fields": (
                    "fee",
                    "previous_fee",
                    "declared_fee",
                )
            }
        ),
        (
            _("Flags"), {
                "fields": (
                    "is_valid",
                    "is_deleted",
                    "errors",
                )
            }
        ),
        (
            _("Sync"), {
                "fields": (
                    "version",
                    "network",
                    "data_version",
                )
            }
        ),
        (
            _("Events List"), {
                "fields": (
                    "entry_events",
                )
            }
        ),
        (
            _("Date & Time"), {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            }
        ),
    )
    list_display = ["operator_id", "owner_address", "is_valid", "is_deleted", "entry_events", "updated_at"]
    search_fields = ["owner", "blockNumber", "public_key"]
    ordering = ["-data_version", "-block_number"]
    formfield_overrides = {
        models.JSONField: {"widget": JSONEditorWidget},
    }
    readonly_fields = (
        "owner",
        "entry_events",
        "version",
        "network",
        "data_version",
        "operator_id",
        "owner_address",
        "owner",
        "fee",
        "previous_fee",
        "declared_fee",
        "is_valid",
        "is_deleted",
        "public_key",
        "created_at",
        "updated_at",
    )
    list_filter = ("network", "version", "data_version", "is_valid", "is_deleted")

    @admin.display(description="Events", ordering="blockNumber")
    def entry_events(self, obj):
        result = []
        i = 0
        for event in obj.events.all():
            i += 1
            result.append(format_html(
                "{}. <a href='{}' target='_blank'>{} ↗️</a>",
                i,
                get_etherscan_link(event.network, "tx/0x%s") % event.transactionHash,
                f"{event.event} ({event.blockNumber})"
            ))
        return mark_safe("<br>".join(result))

    def owner_address(self, obj: OperatorModel):
        return format_html(
            '<a href="{}" target="_blank" style="{}">{} ↗️</a>',
            get_etherscan_link(str(obj.network), 'address/%s') % obj.owner,
            dont_break_style,
            obj.owner
        )


@admin.register(MainnetOperator)
class MainnetOperatorAdmin(TestnetOperatorAdmin):
    pass
