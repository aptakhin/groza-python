from typing import Optional

from groza.queue import BaseQueue


class GrozaClient:
    def __init__(self, name):
        self._name = name

        self._notifications: Optional[BaseQueue] = None

    @property
    def name(self):
        return self._name

    async def install(self, notifications: BaseQueue):
        self._notifications = notifications
