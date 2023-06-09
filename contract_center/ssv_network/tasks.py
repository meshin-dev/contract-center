import time
from typing import Type, List, Union

from django.core.cache import caches
from django.db.models import Q
from django_redis.client import DefaultClient
from redis.lock import Lock

from config import celery_app
from contract_center.contract.models import Sync
from contract_center.ssv_network.models.events import event_models, EventModel
from contract_center.ssv_network.models.operators import OperatorModel, operator_models


@celery_app.task()
def process_raw_events(version: str, network: str, context: str = 'periodic', *args, **kwargs):
    """
    Task to go over all raw events which has been not processed yet and process them
    in chronological order
    :param version:
    :param network:
    :param context:
    :param args:
    :param kwargs:
    :return:
    """
    # Getting default cache
    cache: DefaultClient = caches['default']

    # Lock with 10 seconds timeout
    # This timeout will be extended every time when time-consuming
    # operations are made.
    # It will give guarantees that lock won't be released while the job is running successfully.
    lock_name = f'process_raw_events_{version.lower()}_{network.lower()}'
    lock: Lock = cache.lock(lock_name, timeout=30)

    try:
        if lock.locked():
            return {
                "version": version,
                "network": network,
                "context": context,
                "processed_events": 0,
                'reason': 'Processor is already locked',
                'lock': lock_name
            }
        if not lock.acquire(blocking_timeout=5):
            return {
                "version": version,
                "network": network,
                "context": context,
                "processed_events": 0,
                'reason': 'Could not acquire the lock during 5 seconds',
                'lock': lock_name
            }

        # Find appropriate sync info
        sync = Sync.load(version=version, network=network, enabled=True)
        if not sync:
            return {
                "version": version,
                "network": network,
                "context": context,
                "processed_events": 0,
                'error': 'Sync is not enabled or not created',
                'lock': lock_name
            }

        # Don't count the time spend on database interaction
        lock.reacquire()

        # Get proper model to get events
        event_model: Type[EventModel] = event_models.get(f'{sync.version.lower()}_{sync.network.lower()}')
        if not event_model:
            raise Exception(f'{sync.name} [{context}]: Could not find event model')

        # Get proper model to process operator
        operator_model: Type[OperatorModel] = operator_models.get(f'{sync.version.lower()}_{sync.network.lower()}')
        if not operator_model:
            raise Exception(f'{sync.name} [{context}]: Could not find operator model')

        # TODO: also get models of validators, clusters and accounts

        # It is important in which order the models are in this list
        models: List[Union[Type[OperatorModel]]] = [operator_model]

        # Get events to process
        events_filter = Q(process_status=None) | Q(process_status="\"\"") | Q(process_status="")
        events: List[EventModel] = event_model.objects.filter(events_filter).order_by('blockNumber', 'transactionIndex')[:10000]
        print(f'{sync.name} [{context}]: Fetched {len(events)} events to process')
        lock.reacquire()
        start_time = time.time()

        events_stats = {}
        for event in events:
            time.sleep(0.5)  # TODO: remove
            # It is important in which order the models are in this list
            for model in models:
                if event.event in model.RELATED_EVENTS:
                    process_status = model.process_event(sync=sync, event=event)
                    events_stats[event.event] = events_stats.get(event.event) or {}
                    events_stats[event.event][process_status] = events_stats[event.event].get(process_status) or 0
                    events_stats[event.event][process_status] += 1
                    # event.process_status = process_status
                    # event.save()
                    print(f'{sync.name} [{context}]: Processed event '
                          f'"{event.event}" with status "{process_status}"')

                # Don't count the time spend on database interaction
                if time.time() - start_time > 5:
                    lock.reacquire()
                    start_time = time.time()
                    print(f'{sync.name} [{context}]: Reacquired lock during long lasting synchronization...')

        return {
            "version": version,
            "network": network,
            "context": context,
            "events": events_stats,
            'lock': lock_name,
        }
    finally:
        try:
            if lock.owned():
                lock.release()
        except KeyError:
            pass
        except Exception as e:
            print(f'{sync.name} [{context}]: Can not release lock! Lock: {lock_name}. Error: {e}')
