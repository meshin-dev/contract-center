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
    Helper function to connect to a websocket endpoint with a connection timeout
    :param url: The URL of the websocket endpoint
    :param timeout: The connection timeout value in seconds
    :return: A websocket connection
    """
    return await asyncio.wait_for(websockets.connect(url), timeout=timeout)


async def live_events_listener(sync: Sync, connect_timeout: int, recv_timeout: int):
    """
    Safely listen for live events from a JSON-RPC endpoint via a websocket with failure-tolerant behavior
    :param sync: The Sync object containing configuration details
    :param connect_timeout: The connection timeout value in seconds
    :param recv_timeout: The receive timeout value in seconds
    :return: None
    """
    while True:
        try:
            # Establish a websocket connection
            ws = await connect_with_timeout(sync.node_websocket_address, connect_timeout)
            try:
                await ws.send(json.dumps({
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": [sync.contract_address],
                    }]
                }))

                print(f'{sync.name}: Connecting to contract address: {sync.contract_address}')
                subscription_response = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                print(f'{sync.name}: Subscription result: {subscription_response}')

                # Start receiving updates with timeout
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                        print(f'{sync.name}: Received new event: {message}')

                        # Trigger an immediate sync task
                        task_params = dict(
                            name='contract.sync_events',
                            args=[],
                            kwargs=dict(
                                context='live',
                                name=sync.name,
                            )
                        )
                        print(f'{sync.name}: Triggering immediate sync: {task_params}')
                        app.send_task(**task_params)
                        print(f'{sync.name}: Triggered successfully')
                    except websockets.ConnectionClosedError:
                        print(f"{sync.name}: Connection closed. Retrying...")
                        break
                    except asyncio.TimeoutError:
                        print(f"{sync.name}: Receive operation timed out. Retrying...")
            finally:
                await ws.close()
        except websockets.ConnectionClosedError:
            print(f"{sync.name}: Connection closed. Retrying...")
        except asyncio.TimeoutError:
            print(f"{sync.name}: Connect operation timed out. Retrying...")
        except Exception as e:
            print(f"{sync.name}: An error occurred: {e}")


class Command(BaseCommand):
    """
    Live events listener in threads
    """
    help = 'Listen for live events in all versions and networks and trigger sync tasks when events are received'

    def handle(self, *args, **options):
        # Constantly retry to get any syncs available
        while True:
            syncs = Sync.objects.filter(enabled=True)
            if len(syncs) > 0:
                break
            print(f'No syncs available to start. Retrying in a minute...')
            time.sleep(60)

        while True:
            loop = asyncio.get_event_loop()
            tasks = [
                live_events_listener(
                    sync,
                    connect_timeout=sync.live_events_connect_timeout,
                    recv_timeout=random.randint(int(sync.live_events_read_timeout / 2), sync.live_events_read_timeout)
                ) for sync in syncs
            ]
            try:
                loop.run_until_complete(asyncio.gather(*tasks))
            except KeyboardInterrupt:
                break
            finally:
                loop.close()
