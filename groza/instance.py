import asyncio
from typing import Dict

from groza.client import GrozaClient
from groza.queue import BaseQueue
from groza.queue.asyncio_queue import AsyncioQueue
from groza.server import GrozaServer


class Groza:
    """
    Central link unit connecting server and client parts.
    Can have many server endpoints and many client connections.
    """

    def __init__(self, notifications_instance=None):
        self._servers: Dict[str, GrozaServer] = {}
        self._clients: Dict[str, GrozaClient] = {}

        self._notifications: BaseQueue = (notifications_instance
            if notifications_instance else AsyncioQueue())

    async def add_clients(self, *clients):
        self._clients.update({c.name: await c.install(self._notifications)
                              for c in clients})

    async def add_servers(self, *servers):
        self._servers.update({s.name: await s.install(self._notifications)
                              for s in servers})

    async def loop(self):
        while True:
            while not self._notifications.empty():
                pid, channel, obj_id = await self._notifications.get()

                # TODO: Don't send full sub on same pids
                # TODO: Route different servers sub. Now send all

                for server in self._servers.values():
                    await server.notify_change(pid, channel, obj_id)

            await asyncio.sleep(0.3)

