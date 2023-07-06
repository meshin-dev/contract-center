from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.contract.models import Sync


@admin.register(Sync)
class SyncAdmin(ModelAdmin):
    fieldsets = (
        (
            _('Control'),
            {
                'fields': (
                    'name',
                    'enabled',
                )
            }
        ),
        (
            _('Contract'),
            {
                'fields': (
                    'contract_address',
                    'contract_abi',
                )
            }
        ),
        (
            _('Sync'), {
                'fields': (
                    'last_synced_block_number',
                    'sync_block_range',
                    'contract_genesis_block',
                    'genesis_event_name',
                    'sync_data_version',
                    'active_data_version',
                    'create_new_version',
                    'clear_versions',
                )
            }
        ),
        (
            _('Eth Node'), {
                'fields': (
                    'node_http_address',
                    'node_websocket_address',
                )
            }
        ),
        (
            _('Connection'), {
                'fields': (
                    'live_events_connect_timeout',
                    'live_events_read_timeout',
                )
            }
        ),
        (
            _('Extra'), {
                'fields': (
                    'meta', 'event_names',
                )
            }
        ),
        (
            _('Date & Time'), {
                'fields': (
                    'createdAt',
                    'updatedAt',
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
        'createdAt',
        'updatedAt',
    )

    def save_model(self, request, obj, form, change):
        if 'clear_versions' in form.changed_data:
            obj.clear_versions = True
        elif 'create_new_version' in form.changed_data:
            obj.create_new_version = True
        elif 'enabled' in form.changed_data and form.cleaned_data['enabled'] and not obj.sync_data_version:
            obj.create_new_version = True
        super().save_model(request, obj, form, change)
