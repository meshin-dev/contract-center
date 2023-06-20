from django.core.management.base import BaseCommand

from contract_center.ssv_network import models


class Command(BaseCommand):
    help = 'Deletes all data from a specific model'

    def add_arguments(self, parser):
        parser.add_argument('model', type=str, help='Name of the model to erase data from')

    def handle(self, *args, **options):
        model = options['model']
        [module_name, model_name] = model.split('.')
        model_file = getattr(models, module_name)
        model = getattr(model_file, model_name)
        model.objects.all().delete()
        self.stdout.write(self.style.SUCCESS(f'Data deleted from {model_file}.{model_name} successfully.'))
