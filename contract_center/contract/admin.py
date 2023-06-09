from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.contract.models import Sync


@admin.register(Sync)
class SyncAdmin(ModelAdmin):
    fieldsets = (
        (_("Control"), {"fields": ("name", "enabled",)}),
        (_("Contract"), {"fields": ("contract_address", "contract_abi",)}),
        (_("Sync"), {"fields": ("last_synced_block_number", "sync_block_range",)}),
        (_("Eth Node"), {"fields": ("node_http_address", "node_websocket_address",)}),
        (_("Connection"), {"fields": ("live_events_connect_timeout", "live_events_read_timeout",)}),
        (_("Extra"), {"fields": ("meta", "event_names",)}),
    )
    list_display = ["name", "enabled", ]
    search_fields = ["contract_address", "contract_abi", "node_http_address", "node_websocket_address", "meta",
                     "event_names", ]
    ordering = ["enabled", "name", ]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }
