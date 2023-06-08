from django.db import models
from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.utils.translation import gettext_lazy as _
from django_json_widget.widgets import JSONEditorWidget
from contract_center.ssv_network.models import TestnetV4Operator, MainnetV4Operator
from contract_center.ssv_network.models.events import TestnetV4Event, MainnetV4Event


@admin.register(TestnetV4Event)
class TestnetV4EventAdmin(ModelAdmin):
    fieldsets = (
        (_("Data"), {"fields": (
            "version", "network", "event", "address", "blockNumber", "logIndex", "transactionIndex", "transactionHash",
            "blockHash", "args")}),
        (_("Processing"), {"fields": ("process_status", "errors",)}),
    )
    list_display = ["event", "address", "transactionHash"]
    search_fields = ["event", "address", "transactionHash", "args"]
    ordering = ["blockNumber", "transactionIndex"]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }


@admin.register(MainnetV4Event)
class TestnetV4EventAdmin(ModelAdmin):
    fieldsets = (
        (_("Data"), {"fields": (
            "version", "network", "event", "address", "blockNumber", "logIndex", "transactionIndex", "transactionHash",
            "blockHash", "args")}),
        (_("Processing"), {"fields": ("process_status", "errors",)}),
    )
    list_display = ["event", "address", "transactionHash"]
    search_fields = ["event", "address", "transactionHash", "args"]
    ordering = ["blockNumber", "transactionIndex"]
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }


@admin.register(TestnetV4Operator)
class TestnetV4OperatorAdmin(ModelAdmin):
    fieldsets = (
        (_("Data"), {"fields": (
            "operatorId",
            "version", "network", "ownerAddress", "blockNumber", "transactionIndex", "errors", )}),
    )
    list_display = ["ownerAddress", "blockNumber"]
    search_fields = ["ownerAddress", "blockNumber"]
    ordering = ["-blockNumber", "-transactionIndex"]


@admin.register(MainnetV4Operator)
class MainnetV4OperatorAdmin(ModelAdmin):
    fieldsets = (
        (_("Data"), {"fields": (
            "operatorId",
            "version", "network", "ownerAddress", "blockNumber", "transactionIndex", "errors", )}),
    )
    list_display = ["ownerAddress", "blockNumber"]
    search_fields = ["ownerAddress", "blockNumber"]
    ordering = ["-blockNumber", "-transactionIndex"]
