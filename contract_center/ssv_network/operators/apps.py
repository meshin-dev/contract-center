from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class SSVNetworkConfig(AppConfig):
    name = "contract_center.ssv_network.operators"
    verbose_name = _("SSV.Network Operators")

    def ready(self):
        try:
            import contract_center.ssv_network.operators.signals  # noqa: F401
        except ImportError:
            pass
