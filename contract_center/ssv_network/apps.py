from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class SSVNetworkConfig(AppConfig):
    name = "contract_center.ssv_network"
    verbose_name = _("SSV.Network")

    def ready(self):
        try:
            import contract_center.ssv_network.signals  # noqa: F401
        except ImportError:
            pass
