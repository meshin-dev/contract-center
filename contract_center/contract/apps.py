from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class ContractConfig(AppConfig):
    name = "contract_center.contract"
    verbose_name = _("Contract")

    def ready(self):
        try:
            import contract_center.contract.signals  # noqa: F401
        except ImportError:
            pass
