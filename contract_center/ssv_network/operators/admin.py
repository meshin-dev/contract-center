from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.ssv_network.operators.models import TestnetOperator, MainnetOperator


@admin.register(TestnetOperator)
class TestnetOperatorAdmin(ModelAdmin):
    fieldsets = (
        (
            _("Operator"), {
                "fields": (
                    "operatorId",
                    "publicKey",
                    "ownerAddress",
                )
            }
        ),
        (
            _("Fee"), {
                "fields": (
                    "fee",
                    "previousFee",
                    "declaredFee",
                )
            }
        ),
        (
            _("Flags"), {
                "fields": (
                    "isValid",
                    "isDeleted",
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
            _("Date & Time"), {
                "fields": (
                    "createdAt",
                    "updatedAt",
                )
            }
        ),
    )
    list_display = ["operatorId", "ownerAddress", "isValid", "isDeleted"]
    search_fields = ["ownerAddress", "blockNumber", "publicKey"]
    ordering = ["operatorId", "updatedAt"]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }
    readonly_fields = (
        "ownerAddress",
        "publicKey",
        "createdAt",
        "updatedAt",
    )
    list_filter = ('network', 'version', 'data_version',)


@admin.register(MainnetOperator)
class MainnetOperatorAdmin(TestnetOperatorAdmin):
    pass
