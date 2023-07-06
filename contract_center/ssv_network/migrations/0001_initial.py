# Generated by Django 4.2.2 on 2023-07-06 15:28

from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="TestnetEvent",
            fields=[
                ("event", models.CharField(max_length=255, verbose_name="Event name")),
                ("logIndex", models.IntegerField(verbose_name="Log Index")),
                ("transactionIndex", models.IntegerField(verbose_name="Transaction Index")),
                (
                    "transactionHash",
                    models.CharField(
                        max_length=255, primary_key=True, serialize=False, unique=True, verbose_name="Transaction Hash"
                    ),
                ),
                ("address", models.CharField(max_length=255, verbose_name="Address")),
                ("blockHash", models.CharField(max_length=255, verbose_name="Block Hash")),
                ("blockNumber", models.CharField(max_length=255, verbose_name="Block Number")),
                ("args", models.JSONField(verbose_name="Arguments")),
                (
                    "process_status",
                    models.CharField(
                        blank=True, default=None, max_length=255, null=True, verbose_name="Process Status"
                    ),
                ),
                ("errors", models.JSONField(blank=True, default=list, verbose_name="Errors")),
                ("version", models.CharField(max_length=255, verbose_name="Version")),
                ("network", models.CharField(max_length=255, verbose_name="Network")),
                ("data_version", models.BigIntegerField(verbose_name="Data Version")),
                ("createdAt", models.DateTimeField(auto_now_add=True)),
                ("updatedAt", models.DateTimeField(auto_now=True)),
            ],
            options={
                "abstract": False,
                "unique_together": {("transactionHash", "network", "version", "data_version")},
            },
        ),
        migrations.CreateModel(
            name="MainnetEvent",
            fields=[
                ("event", models.CharField(max_length=255, verbose_name="Event name")),
                ("logIndex", models.IntegerField(verbose_name="Log Index")),
                ("transactionIndex", models.IntegerField(verbose_name="Transaction Index")),
                (
                    "transactionHash",
                    models.CharField(
                        max_length=255, primary_key=True, serialize=False, unique=True, verbose_name="Transaction Hash"
                    ),
                ),
                ("address", models.CharField(max_length=255, verbose_name="Address")),
                ("blockHash", models.CharField(max_length=255, verbose_name="Block Hash")),
                ("blockNumber", models.CharField(max_length=255, verbose_name="Block Number")),
                ("args", models.JSONField(verbose_name="Arguments")),
                (
                    "process_status",
                    models.CharField(
                        blank=True, default=None, max_length=255, null=True, verbose_name="Process Status"
                    ),
                ),
                ("errors", models.JSONField(blank=True, default=list, verbose_name="Errors")),
                ("version", models.CharField(max_length=255, verbose_name="Version")),
                ("network", models.CharField(max_length=255, verbose_name="Network")),
                ("data_version", models.BigIntegerField(verbose_name="Data Version")),
                ("createdAt", models.DateTimeField(auto_now_add=True)),
                ("updatedAt", models.DateTimeField(auto_now=True)),
            ],
            options={
                "abstract": False,
                "unique_together": {("transactionHash", "network", "version", "data_version")},
            },
        ),
    ]
