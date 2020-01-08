from groza.client import GrozaClient
from groza.queue import BaseQueue
from groza.storage import GrozaStorage


class GrozaStorageClient(GrozaClient):
    def __init__(self, name: str, storage: GrozaStorage):
        super().__init__(name)
        self._storage: GrozaStorage = storage

    async def install(self, notifications: BaseQueue):
        await super().install(notifications)
        self._storage._notifications = notifications # FIXME: this strange piece
