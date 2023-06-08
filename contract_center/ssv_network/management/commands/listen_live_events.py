import json
import time
import random
import asyncio
import websockets
from config.celery_app import app
from contract_center.contract.models import Sync
from django.core.management.base import BaseCommand


async def connect_with_timeout(url: str, timeout: int):
    """
    Helper function to connect to a websocket endpoint with connection timeout
    :param url:
    :param timeout:
    :return:
    """
    return await asyncio.wait_for(websockets.connect(url), timeout=timeout)


async def live_events_listener(sync: Sync, connect_timeout: int, recv_timeout: int):
    """
    Safely listen for live events from JSON-RPC endpoint via websocket with failure-tolerant behavior
    :param sync:
    :param connect_timeout:
    :param recv_timeout:
    :return:
    """
    while True:
        try:
            ws = await connect_with_timeout(sync.beacon_node_websocket_address, connect_timeout)
            try:
                await ws.send(json.dumps({
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": [sync.contract_address],
                    }]
                }))

                print(f'{sync.version} {sync.network}: Connecting to contract address: {sync.contract_address}')
                subscription_response = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                print(f'{sync.version} {sync.network}: Subscription result: {subscription_response}')

                # Start receiving any updates with timeout
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                        print(f'{sync.version} {sync.network}: Received new event: {message}')
                        task_params = dict(
                            name='contract_center.contract.tasks.sync_contract_raw_events',
                            args=[],
                            kwargs=dict(
                                version=sync.version,
                                network=sync.network,
                                context='live',
                            )
                        )
                        print(f'{sync.version} {sync.network}: Triggering immediate sync: {task_params}')
                        app.send_task(**task_params)
                        print(f'{sync.version} {sync.network}: Triggered successfully')
                    except websockets.ConnectionClosedError:
                        print(f"{sync.version} {sync.network}: Connection closed. Retrying...")
                        break
                    except asyncio.TimeoutError:
                        print(f"{sync.version} {sync.network}: Receive operation timed out. Retrying...")
            finally:
                await ws.close()
        except websockets.ConnectionClosedError:
            print(f"{sync.version} {sync.network}: Connection closed. Retrying...")
        except asyncio.TimeoutError:
            print(f"{sync.version} {sync.network}: Connect operation timed out. Retrying...")
        except Exception as e:
            print(f"{sync.version} {sync.network}: An error occurred: {e}")


class Command(BaseCommand):
    """
    Live events listener in threads
    """
    help = 'Listen for live events in all versions and networks and trigger sync tasks when events coming'

    def handle(self, *args, **options):

        # Constantly retry to get any syncs available
        while True:
            syncs = Sync.objects.filter(enabled=True)
            if len(syncs):
                break
            print(f'No syncs available to start. Retrying in a minute...')
            time.sleep(60)

        while True:
            loop = asyncio.get_event_loop()
            tasks = [live_events_listener(
                sync,
                connect_timeout=10,
                recv_timeout=random.randint(60, 120)
            ) for sync in syncs]
            try:
                loop.run_until_complete(asyncio.gather(*tasks))
            except KeyboardInterrupt:
                break
            finally:
                loop.close()
