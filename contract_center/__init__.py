__version__ = "0.1.0"
__version_info__ = tuple(int(num) if num.isdigit() else num for num in __version__.replace("-", ".", 1).split("."))

from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules


class ContractCenterConfig(AppConfig):
    name = 'contract_center'

    def ready(self):
        autodiscover_modules('receivers')


default_app_config = ContractCenterConfig
