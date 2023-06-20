from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget

from contract_center.ssv_network.operators.models import TestnetV4Operator, MainnetV4Operator


@admin.register(TestnetV4Operator)
class TestnetV4OperatorAdmin(ModelAdmin):
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
            _("Transaction Info"), {
                "fields": (
                    "blockNumber",
                    "transactionIndex",
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
            _("Contract"), {
                "fields": (
                    "version",
                    "network",
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
    ordering = ["-blockNumber", "-transactionIndex"]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }
    readonly_fields = (
        "createdAt",
        "updatedAt",
    )


@admin.register(MainnetV4Operator)
class MainnetV4OperatorAdmin(TestnetV4OperatorAdmin):
    pass
