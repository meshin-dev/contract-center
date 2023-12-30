from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.contract.models import Sync


@admin.register(Sync)
class SyncAdmin(ModelAdmin):
    fieldsets = (
        (
            _('Sync Settings'),
            {
                'fields': (
                    'name',
                    'enabled',
                    'process_enabled',
                    'last_synced_block_number',
                    'process_from_block',
                    'process_block_range',
                    'sync_block_range',
                    'sync_block_range_effective',
                    'sync_block_range_effective_multiplier',
                    'contract_genesis_block',
                    'genesis_event_name',
                )
            }
        ),
        (
            _('Data Versioning'),
            {
                'fields': (
                    'sync_data_version',
                    'active_data_version',
                    'create_new_version',
                    'clear_versions',
                    'auto_switch_to_synced_version',
                )
            }
        ),
        (
            _('Contract Settings'),
            {
                'fields': (
                    'contract_address',
                    'contract_abi',
                )
            }
        ),
        (
            _('Eth Node Settings'), {
                'fields': (
                    'node_http_address',
                    'node_websocket_address',
                )
            }
        ),
        (
            _('Connection Settings'), {
                'fields': (
                    'live_events_connect_timeout',
                    'live_events_read_timeout',
                )
            }
        ),
        (
            _('Extra Settings'), {
                'fields': (
                    'meta', 'event_names',
                )
            }
        ),
        (
            _('Date & Time'), {
                'fields': (
                    'created_at',
                    'updated_at',
                )
            }
        ),
    )
    list_display = ['name', 'enabled', 'sync_data_version', 'active_data_version']
    search_fields = [
        'contract_address',
        'contract_abi',
        'node_http_address',
        'node_websocket_address',
        'meta',
        'event_names',
    ]
    ordering = ['enabled', 'name', ]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }
    readonly_fields = (
        'created_at',
        'updated_at',
    )

    def save_model(self, request, obj, form, change):
        if 'clear_versions' in form.changed_data:
            obj.clear_versions = True
            obj.process_from_block = 0
        elif 'create_new_version' in form.changed_data:
            obj.create_new_version = True
            obj.process_from_block = 0
        elif 'enabled' in form.changed_data and form.cleaned_data['enabled'] and not obj.sync_data_version:
            obj.create_new_version = True
            obj.process_from_block = 0
        super().save_model(request, obj, form, change)
