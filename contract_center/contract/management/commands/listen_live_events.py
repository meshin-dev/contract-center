import asyncio
import json
import logging
import random
import time

import websockets
from django.core.management.base import BaseCommand

from contract_center.contract.models import Sync
from contract_center.contract.tasks import EventsFetchTask

logger = logging.getLogger(__name__)


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
                connection_options = {
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": [sync.contract_address],
                    }]
                }
                logger.debug(f'Contract events live listener connection options: {connection_options}')
                await ws.send(json.dumps(connection_options))

                logger.info(f'{sync.name}: Connecting...')
                subscription_response = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                logger.info(f'{sync.name}: Subscription result: {subscription_response}')

                # Start receiving updates with timeout
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                        logger.info(f'{sync.name}: Received new event: {message}')
                        params = dict(
                            args=[
                                sync.name,
                                dict(
                                    type='live'
                                )
                            ],
                            kwargs=dict(
                                name=sync.name,
                                context=dict(
                                    type='live'
                                )
                            )
                        )
                        EventsFetchTask().apply_async(**params)
                        logger.info(f'{sync.name}: Triggered live sync: {params}')
                    except websockets.ConnectionClosedError:
                        logger.error(f"{sync.name}: Connection closed. Retrying...")
                        break
                    except asyncio.TimeoutError:
                        logger.debug(f"{sync.name}: Receive operation timed out. Retrying...")
            finally:
                await ws.close()
        except websockets.ConnectionClosedError:
            logger.error(f"{sync.name}: Connection closed. Retrying...")
        except asyncio.TimeoutError:
            logger.error(f"{sync.name}: Connect operation timed out. Retrying...")
        except Exception as e:
            logger.error(f"{sync.name}: An error occurred: {e}")


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
            logger.warning(f'No syncs available to start. Retrying in a minute...')
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
