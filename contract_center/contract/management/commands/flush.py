import importlib
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = 'Deletes all data from a specific models'

    def add_arguments(self, parser):
        parser.add_argument('--models', type=str, help='Full path to the models to erase data in. Format: '
                                                       'my.module:modelName,my.module2:modelName2')
        parser.add_argument('--data-versions', type=str,
                            help='Data versions to delete. Format: <from>[-<to>]')

    def handle(self, *args, **options):
        models = options['models'].split(',')
        data_versions = options['data_versions'].split('-')

        if not len(data_versions):
            raise CommandError('No data versions provided')

        for model in models:
            module_name = model.split('.')
            model_name = module_name[-1]
            module_name = '.'.join(module_name[:-1])
            module = importlib.import_module(module_name)
            model = getattr(module, model_name)
            model.objects.filter(
                data_version__gte=data_versions[0],
                data_version__lte=data_versions[-1],
            ).delete()
            self.stdout.write(self.style.SUCCESS(f'Data deleted from {module_name}.{model_name} successfully.'))
