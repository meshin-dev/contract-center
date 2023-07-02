import logging
from dataclasses import dataclass
from typing import List, Union, Dict, Tuple

from dataclasses_json import dataclass_json

from config.celery_app import app
from contract_center.contract.library.tasks import SmartTask, TaskResult
from contract_center.contract.models.sync import Sync, GENESIS_EVENT_NAME_DEFAULT
from contract_center.contract.receivers import EventReceiverResult
from contract_center.contract.signals import contract_fetched_events_signal
from contract_center.contract.web3.contract import Web3Contract

logger = logging.getLogger(__name__)


@dataclass_json
@dataclass
class EventsFetchTaskResult(TaskResult):
    """
    Dataclass representing the result of the EventsFetchTask.
    """
    block_from: int = 0
    block_to: int = 0
    current_block: int = 0
    total_events: int = 0


class EventsFetchTask(SmartTask):
    name = 'contract.events_fetch'
    queue = 'queue_events_fetch'

    sync: Sync = None
    block_from_back_offset = 2
    minimum_blocks_to_fetch = 10
    web3_contract: Web3Contract = None

    def get_lock_name(self) -> str:
        return f'{self.name}.{self.sync.name}'

    @property
    def contract(self) -> Web3Contract:
        """
        Get the contract instance associated with Sync
        :return:
        """
        if not self.web3_contract:
            self.web3_contract = Web3Contract(
                name=self.sync.name,
                node_http_address=self.sync.node_http_address,
                node_websocket_address=self.sync.node_websocket_address,
                contract_address=self.sync.contract_address,
                contract_abi=self.sync.contract_abi,
            )
        return self.web3_contract

    def get_sync(self, **kwargs) -> Union[Sync, EventsFetchTaskResult]:
        """
        Tries to load sync information based on the provided keyword arguments. If the Sync is not found or not enabled,
        it returns a task result indicating the failure.

        :param kwargs: Keyword arguments used to load the Sync.

        :return: A Sync instance if it is found and enabled, else an instance of EventsFetchTaskResult containing
                 the task failure details.
        """
        sync = Sync.load(**kwargs)
        if not sync:
            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                reason=f'Sync is not enabled or does not exist: {kwargs}'
            )
        return sync

    def get_blocks(self) -> Tuple[int, int, int]:
        # Start from the block after the last synced block or from the genesis block
        block_last_synced = -1
        try:
            block_last_synced = max(self.sync.last_synced_block_number, self.sync.contract_genesis_block)
        except ValueError:
            pass

        # If was empty or -1 - then take genesis block
        if block_last_synced <= 0:
            block_last_synced = self.contract.get_genesis_block_number(
                genesis_event_name=self.sync.genesis_event_name or GENESIS_EVENT_NAME_DEFAULT
            )
            if not self.sync.contract_genesis_block:
                self.sync.contract_genesis_block = block_last_synced
                self.sync.save(update_fields=['contract_genesis_block'])

        current_block = int(self.contract.web3_http.eth.get_block('latest').get('number'))
        block_from = block_last_synced + 1 \
            if block_last_synced >= 0 \
            else current_block

        # Calculate block_to as either block_from plus minimum_blocks_to_fetch or current_block - 2
        # whichever is less, ensuring block_to is never greater than current_block.
        block_to = min(
            block_from + (self.sync.sync_block_range or self.minimum_blocks_to_fetch),
            current_block
        )

        # If block_to equals current_block, decrement block_from by 2 to ensure we don't miss data due
        # to potential delays in event fetching from the Ethereum node
        if block_to == current_block:
            block_from -= self.block_from_back_offset

        return block_from, block_to, current_block

    def run(self, *args, **kwargs):
        """
        This is the main execution method of the EventsFetchTask class. It is responsible for fetching events from an Ethereum
        contract. The method follows these steps:

        1. Tries to acquire a lock to ensure no other instance is currently fetching events.
        2. Retrieves the sync information and checks its validity.
        3. Determines the block range to fetch events from.
        4. Fetches events from the Ethereum contract within the block range determined in step 3.
        5. If events are fetched, it sends a signal to receivers with the fetched events.
        6. Processes any errors occurred during the above steps, logs them, and returns corresponding results.
        7. In the end, releases the lock and if there are more blocks to fetch, it triggers the same task again asynchronously.
        """
        events = []
        block_from = block_to = current_block = None
        can_self_call = False
        try:
            # Get sync entry from kwargs
            sync_name = kwargs.get('name', None)
            if not sync_name:
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock='',
                    reason=f'Wrong parameters. Provide {{"name": "..."}} in task Keyword Arguments. '
                           f'kwargs: {kwargs}',
                ).to_dict()

            # Get sync object
            self.sync = self.get_sync(name=sync_name)
            if isinstance(self.sync, EventsFetchTaskResult):
                return self.sync.to_dict()

            self.log(f'Found sync for kwargs: {kwargs}', log_method=logger.debug)

            # Check if sync is enabled
            if not bool(self.sync.enabled):
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Sync disabled: {self.sync.name}',
                ).to_dict()

            # Check proper context structure
            self.context = kwargs.get('context', self.context)
            if not isinstance(self.context, dict) or not self.context.get('type'):
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock='',
                    reason=f'Wrong parameters. '
                           f'Provide {{"context": {{"type": "...", ...}}}} in task Keyword Arguments. '
                           f'kwargs: {kwargs}',
                ).to_dict()

            # Try to acquire the lock for this task
            try:
                self.get_lock_manager().lock_acquire()
            except Exception as e:
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=str(e),
                ).to_dict()

            # Wait for blocks range calculation
            self.get_lock_manager().submit(lambda: self.get_blocks())
            self.get_lock_manager().keep_alive(raise_exceptions=True)

            # Get blocks data
            block_from, block_to, current_block = self.get_lock_manager().result(clean=True).result()
            self.log(
                f'Found block range to sync events: {block_from}-{block_to}. Current block: {current_block}',
                log_method=logger.debug
            )

            # Fetch events
            events: List[Dict] = []
            fetch_params = dict(
                events=self.sync.event_names,
                block_from=block_from,
                block_to=block_to,
            )
            try:
                self.get_lock_manager().submit(lambda: self.contract.events_fetch(**fetch_params))
                self.get_lock_manager().keep_alive(raise_exceptions=True)
                events: List[Dict] = self.get_lock_manager().result(clean=True).result()
                self.log(f'Fetched {len(events)} new events', log_method=logger.debug)
            except Exception as e:
                self.log(e)
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Could not fetch new events',
                    error=str(e),
                    block_from=block_from,
                    block_to=block_to,
                    current_block=current_block,
                ).to_dict()

            # If no events are fetched, update last_synced_block_number and return
            if not len(events):
                self.sync.last_synced_block_number = block_to - self.block_from_back_offset
                self.get_lock_manager().submit(lambda: self.sync.save(update_fields=['last_synced_block_number']))
                self.get_lock_manager().keep_alive(raise_exceptions=True)

                can_self_call = True
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    block_from=block_from,
                    block_to=block_to,
                    current_block=current_block,
                ).to_dict()

            # Trigger signal to sync new events
            try:
                self.get_lock_manager().submit(lambda: contract_fetched_events_signal.send(
                    sender=self.__name__,
                    instance=self,
                    params=fetch_params,
                    events=events,
                ))
                self.get_lock_manager().keep_alive(raise_exceptions=True)
                results: List[EventReceiverResult] = self.get_lock_manager().result(clean=True).result()
                can_self_call = True
            except Exception as e:
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Could not send new events signal to receivers',
                    error=str(e),
                    block_from=block_from,
                    block_to=block_to,
                    current_block=current_block,
                ).to_dict()

            # If no receivers are listening for new events, return without saving last synced block number
            if not len(results):
                return EventsFetchTaskResult(
                    task=self.name,
                    context=self.context,
                    lock=self.get_lock_name(),
                    reason=f'Nobody is listening for new events',
                    block_from=block_from,
                    block_to=block_to,
                    current_block=current_block,
                ).to_dict()

            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                block_from=block_from,
                block_to=block_to,
                current_block=current_block,
                total_events=len(events),
                results=[result[1].to_dict() for result in results]
            ).to_dict()

        except Exception as e:
            self.log('Can not fetch events')
            self.log(e)
            return EventsFetchTaskResult(
                task=self.name,
                context=self.context,
                lock=self.get_lock_name(),
                block_from=block_from,
                block_to=block_to,
                current_block=current_block,
                total_events=len(events),
            ).to_dict()
        finally:
            # Check if we need to call self again
            self.get_lock_manager().close()
            if current_block and block_to and current_block - block_to > self.minimum_blocks_to_fetch and can_self_call:
                kwargs = {
                    **kwargs,
                    "context": {
                        **self.context,
                        'type': 'periodic'
                    }
                }
                EventsFetchTask().apply_async(
                    args=args,
                    kwargs=kwargs,
                )


app.register_task(EventsFetchTask)
