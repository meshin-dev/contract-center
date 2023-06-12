import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import List, Union, Dict

from django.core.cache import caches
from django_redis.client import DefaultClient
from redis.lock import Lock
from torch.multiprocessing import Manager

from config import celery_app
from contract_center.contract.models.sync import Sync
from contract_center.contract.receivers import ContractEventsReceiverResult
from contract_center.contract.signals import contract_fetched_events_signal
from contract_center.contract.web3.contract import Web3Contract
from contract_center.contract.web3.events import sanitize_events

logger = logging.getLogger(__name__)


def get_sync_info(context: str, **kwargs) -> Union[Sync, Dict]:
    sync = Sync.load(**kwargs)
    if not sync:
        return {
            "context": context,
            "kwargs": kwargs,
            'error': 'Sync is not enabled or does not exist',
        }
    return sync


def fetch_events_worker(contract: Web3Contract,
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
            logger.error(f'{sync.name} [{context}]: Exception occurred during events fetch')
            logger.exception(e)
            logger.warning(f'{sync.name} [{context}]: Retrying in a second...')
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
            logger.debug(f'{sync.name} [{context}]: Reacquired lock during long lasting synchronization...')
        except Exception as e:
            raise
        # Sleep for 5 seconds before reacquiring the lock
        time.sleep(5)


@celery_app.task(
    bind=True,
    name='contract.fetch_events',
    queue='queue_events_fetch'
)
def fetch_events(self, context: str = 'periodic', *args, **kwargs):
    # Find appropriate sync info
    sync_kwargs = {**kwargs, 'enabled': True}
    sync = get_sync_info(context, **sync_kwargs)
    if not isinstance(sync, Sync):
        logger.debug(f'[{context}] Could not find sync for arguments: {kwargs}')
        return sync

    # Getting default cache
    cache: DefaultClient = caches['default']

    # Lock with 15 seconds timeout (1 epoch)
    # This timeout will be reacquired every time when time-consuming
    # operations are made.
    # It will give guarantees that lock won't be released while the job is running successfully.
    # Build lock name based on sync slug name
    lock_name = f'contract.fetch_events.{sync.name}'
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

        logger.debug(f'{sync.name} [{context}]: Acquired the lock: {lock_name}')

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
                    fetch_events_worker,
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
                # Graceful shutdown thread pool
                executor.shutdown()

                logger.error(f'{sync.name} [{context}]: Got an exception during fetching events. Stopping now!')
                logger.exception(e)

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
        new_events_signal_future = executor.submit(lambda: contract_fetched_events_signal.send(
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

        # Check out error from signal receivers and return error in case of failure
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

        # Collect receivers results
        results: List[ContractEventsReceiverResult] = new_events_signal_future.result()

        if not len(results):
            logger.warning(f'{sync.name} [{context}]: No results from receivers. '
                           f'Looks like nobody is listening for new events')
            return {
                'context': context,
                'sync_kwargs': sync_kwargs,
                'saved_events': 0,
                'error': f'Nobody is listening for new events',
                'lock': lock_name,
                'block_from': from_block,
                'block_to': to_block,
            }

        # Reacquire the lock after few final save iterations
        lock.reacquire()

        # Check if any receiver saved these events
        saved_new_events = False
        for result in results:
            result = result[1]
            saved_new_events = bool(result.saved_total)
            break

        # Even if receivers saved last synced block number it's ok to do that
        # because next round of sync will be just continuing from where receiver failed
        # On the other hand, if there were no errors during receivers work,
        # Here last synced block number should be saved with a different logic.
        if to_block - from_block > 5 and not saved_new_events:
            # If there were no events - it doesn't mean that there is no events in blockchain,
            # because in the past there were situations when for example sqlalchemy returned empty results
            # but there were events by the fact. And only after some period of time these events appeared in their API.
            # So here it saves always last block number with offset of 5 blocks back, to make sure to not miss it.
            sync.last_synced_block_number = to_block - 5
            sync.save()
            lock.reacquire()
        elif saved_new_events:
            # If events were saved, it saves last block number with offset of 1
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
    except Exception as taskError:
        logger.error(f'{sync.name} [{context}]: Can not run sync task')
        logger.exception(taskError)
        return {
            'context': context,
            'sync_kwargs': sync_kwargs,
            'saved_events': 0,
            'error': f'{taskError}',
            'lock': lock_name
        }
    finally:
        try:
            if lock.owned():
                lock.release()
        except KeyError:
            pass
        except Exception as e:
            logger.error(f'{sync.name} [{context}]: Can not release lock! Lock: {lock_name}. Error: {e}')
