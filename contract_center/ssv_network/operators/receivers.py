import base64
from typing import Type, Dict, Union

from cryptography.hazmat.primitives import serialization
from django.dispatch import receiver
from eth_abi import decode

from contract_center.contract.receivers import EventReceiverResult
from contract_center.ssv_network.models import EventModel
from contract_center.ssv_network.operators.models.operators import OperatorModel, get_operator_model
from contract_center.ssv_network.signals import event_process_signal
from contract_center.ssv_network.tasks import EventsProcessTask, ProcessStatus

# TODO:
# - [ ] Move all validations to validators.py
# - [ ] Move all logic to Strategy classes
# - [ ] Move all constants to constants.py
# - [ ] Move all utils to utils.py


def validate_key(base64_key: str) -> Union[bool, str]:
    """
    Validate if given key is valid RSA public key.
    :param base64_key:
    :return:
    """
    key_bytes = base64.b64decode(base64_key)
    try:
        serialization.load_pem_public_key(key_bytes)
        return True
    except Exception as e:
        return e.args[0]


@receiver(event_process_signal)
def event_process_signal_receiver(
    sender: Type[EventsProcessTask],
    instance: EventsProcessTask,
    event: EventModel,
    **kwargs
) -> EventReceiverResult:
    # Initialize result
    result = EventReceiverResult(
        receiver=f'{__name__}.{event_process_signal_receiver.__name__}',
        saved_total=0,
        saved_events={},
    )

    # Validate if event can be processed
    if not str(instance.sync.name).startswith('ssv_network'):
        return result
    if event.process_status == ProcessStatus.PROCESSED.value:
        return result
    if event.event not in dir(OperatorModel.Sync.Events):
        return result

    # Get operator model for given network
    operator_model: Type[OperatorModel] = get_operator_model(event.network)

    # Using this model, get or create the operator
    event_args: Dict = event.args
    event_args['errors'] = []

    # Deserialize public key if it's present
    if event_args.get('publicKey'):
        public_key_bytes = bytes.fromhex(str(event_args['publicKey']).removeprefix('0x'))
        event_args['publicKey'] = decode(['string'], public_key_bytes)[0]
        validated = validate_key(event_args['publicKey'])
        if validated is not True:
            event_args['errors'].append(dict(
                field='publicKey',
                message=validated,
            ))

    operator_data = dict(
        operator_id=event_args.get('operatorId'),
        network=instance.sync.meta.get('network'),
        version=instance.sync.meta.get('version'),
        data_version=instance.sync.sync_data_version,
    )

    updated = False
    if event.event == OperatorModel.Sync.Events.OperatorAdded.value:
        operator, created = operator_model.objects.get_or_create(
            **operator_data,
            defaults=dict(
                **operator_data,
                fee=event_args.get('fee'),
                owner=event_args.get('owner'),
                block_number=event.blockNumber,
                errors=event_args.get('errors'),
                public_key=event_args.get('publicKey'),
            )
        )
        if not created and operator:
            operator.is_deleted = False
            operator.fee = event_args.get('fee'),

            # TODO: react on fee related events and change operator fee values accordingly
            # operator.declared_fee = event_args.get('declared_fee'),
            # operator.previous_fee = event_args.get('previous_fee'),

            operator.owner = event_args.get('owner')
            operator.block_number = event.blockNumber
            operator.public_key = event_args.get('publicKey')

        operator.events.add(event)
        operator.save()
        updated = True
    elif event.event == OperatorModel.Sync.Events.OperatorRemoved.value:
        operator = operator_model.objects.filter(**operator_data).first()
        if operator:
            operator.is_deleted = True
            operator.block_number = event.blockNumber
            operator.events.add(event)
            operator.save()
            updated = True

    # Update event process status
    if updated:
        result.saved_events[event.event] = result.saved_events.get(event.event) or 0
        result.saved_events[event.event] += 1
        result.saved_total += 1

    return result
