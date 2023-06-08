from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class UsersConfig(AppConfig):
    name = "contract_center.users"
    verbose_name = _("Users")

    def ready(self):
        try:
            import contract_center.users.signals  # noqa: F401
        except ImportError:
            pass
