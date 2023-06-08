import time
from redis.lock import Lock
from config import celery_app
from django.core.cache import caches
from django.db.models import Model, Q
from typing import Type, List, Union, Dict
from torch.multiprocessing import Manager
from django_redis.client import DefaultClient
from contract_center.contract.models.sync import Sync
from contract_center.contract.web3.contract import Web3Contract
from contract_center.ssv_network.models.events import EventModel
from contract_center.contract.web3.events import sanitize_events
from contract_center.ssv_network.models.events import event_models
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from contract_center.contract.signals import contract_new_events_signal
from contract_center.contract.receivers import ContractEventsReceiverResult
from contract_center.ssv_network.models.operators import operator_models, OperatorModel


def get_sync_info(context: str, **kwargs) -> Union[Sync, Dict]:
    sync = Sync.load(**kwargs)
    if not sync:
        return {
            "context": context,
            "kwargs": kwargs,
            'error': 'Sync is not enabled or does not exist',
        }
    return sync


def fetch_events(contract: Web3Contract,
                 sync: Sync,
                 context: str,
                 event_name: str,
                 from_block: int,
                 to_block: int,
                 max_retries: int = 3):

    while max_retries:
        try:
            return getattr(contract.http_contract.events, event_name).get_logs(
                fromBlock=from_block,
                toBlock=to_block,
            )
        except Exception as e:
            print(f'{sync.name} [{context}]: Exception occurred during events fetch: {e}: {e.args}')
            print(f'{sync.name} [{context}]: Retrying in a second...')
            time.sleep(1)
            max_retries -= 1
            continue


def reacquire_lock(future: Future, lock: Lock, context: str, sync: Sync):
    while True:
        if future.done():
            # Stop reacquiring the lock if the future has a result
            break
        try:
            lock.reacquire()
            print(f'{sync.name} [{context}]: Reacquired lock during long lasting synchronization...')
        except Exception as e:
            raise
        # Sleep for 5 seconds before reacquiring the lock
        time.sleep(5)


@celery_app.task(
    bind=True,
    name='contract.sync_events'
)
def sync_events(self, context: str = 'periodic', *args, **kwargs):
    # Find appropriate sync info
    sync_kwargs = {**kwargs, 'enabled': True}
    sync = get_sync_info(context, **sync_kwargs)
    if not isinstance(sync, Sync):
        return sync

    # Getting default cache
    cache: DefaultClient = caches['default']

    # Lock with 15 seconds timeout (1 epoch)
    # This timeout will be reacquired every time when time-consuming
    # operations are made.
    # It will give guarantees that lock won't be released while the job is running successfully.
    # Build lock name based on sync slug name
    lock_name = f'contract.sync_events.{sync.name}'
    lock: Lock = cache.lock(lock_name, timeout=15)

    try:
        if lock.locked():
            return {
                'context': context,
                'sync_kwargs': sync_kwargs,
                'saved_events': 0,
                'reason': 'Sync is already locked',
                'lock': lock_name
            }
        if not lock.acquire(blocking_timeout=5):
            return {
                'context': context,
                'sync_kwargs': sync_kwargs,
                'saved_events': 0,
                'reason': 'Could not acquire the lock during 5 seconds',
                'lock': lock_name
            }

        # Don't count the time spend on database interaction or lock waiting
        lock.reacquire()

        # Get all required parts to interact with the chain
        contract: Web3Contract = Web3Contract(
            node_http_address=sync.node_http_address,
            node_websocket_address=sync.node_websocket_address,
            contract_address=sync.contract_address,
            contract_abi=sync.contract_abi,
        )

        # Thread-safe list
        manager = Manager()
        all_events = manager.list()

        from_block = int(sync.last_synced_block_number or contract.get_genesis_block_number())
        latest_block = int(contract.http_web3.eth.get_block('latest').get('number'))
        to_block = int(min(from_block + sync.sync_block_range if sync.sync_block_range else latest_block, latest_block))

        # Fetch events in parallel using multiprocessing pool
        max_workers = min(5, len(list(sync.event_names)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            events_result_future = {
                executor.submit(
                    fetch_events,
                    **dict(
                        contract=contract,
                        sync=sync,
                        context=context,
                        event_name=str(event_name),
                        from_block=from_block,
                        to_block=to_block
                    )
                ) for event_name in sync.event_names
            }
            try:
                for future in as_completed(events_result_future):
                    all_events.extend(future.result())
            except Exception as e:
                print(f'{sync.name} [{context}]: Got an exception during fetching events. '
                      f'Stopping now. Error: {e}: {e.args}')

                # Graceful shutdown thread pool
                executor.shutdown()

                # Return error from task
                return {
                    'context': context,
                    'sync_kwargs': sync_kwargs,
                    'saved_events': 0,
                    'error': f'Could not fetch events: {e}',
                    'lock': lock_name
                }

        # Sanitize and sort all events
        all_events = sanitize_events(all_events)
        lock.reacquire()

        # Submit the long_running_function to the executor
        executor = ThreadPoolExecutor(max_workers=2)
        new_events_signal_future = executor.submit(lambda: contract_new_events_signal.send(
            sender=sync.__class__,
            context=context,
            sync=sync,
            events=all_events,
        ))

        # Reacquire the lock in parallel
        reacquire_lock(
            future=new_events_signal_future,
            lock=lock,
            context=context,
            sync=sync,
        )
        error = new_events_signal_future.exception()
        if error:
            # Return error from task
            return {
                'context': context,
                'sync_kwargs': sync_kwargs,
                'saved_events': 0,
                'error': f'Could not send new events signal to receivers: {error} {error.args}',
                'lock': lock_name
            }

        results: List[ContractEventsReceiverResult] = new_events_signal_future.result()

        # Reacquire the lock after few final save iterations
        lock.reacquire()

        # Check if any receiver saved these events
        saved_new_events = False
        for result in results:
            result = result[1]
            if result.saved_total:
                saved_new_events = bool(result.saved_total)
                break

        # Apply the same logic as in previous version of CC:
        #  - If no data came then don't save last block number
        #  - AND if block range is more than 5 blocks - then move block from closer to block_to
        if to_block - from_block > 5 and not saved_new_events:
            sync.last_synced_block_number = to_block - 5
            sync.save()
            lock.reacquire()
        elif saved_new_events:
            sync.last_synced_block_number = to_block - 1
            sync.save()
            lock.reacquire()

        response = {
            'context': context,
            'sync_kwargs': sync_kwargs,
            'total_events': len(all_events),
            'saved_last_block_number': sync.last_synced_block_number,
            'lock': lock_name,
            'block_from': from_block,
            'block_to': to_block,
            'results': [result[1].to_dict() for result in results],
        }
        return response
    finally:
        try:
            if lock.owned():
                lock.release()
        except KeyError:
            pass
        except Exception as e:
            print(f'{sync.name} [{context}]: Can not release lock! Lock: {lock_name}. Error: {e}')


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
        event_model: Type[Model] = event_models.get(f'{sync.version.lower()}_{sync.network.lower()}')
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
