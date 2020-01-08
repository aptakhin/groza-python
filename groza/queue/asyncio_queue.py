import asyncio

from groza.queue import BaseQueue


class AsyncioQueue(BaseQueue):
    def __init__(self):
        self._q = asyncio.Queue()

    async def put(self, item):
        await self._q.put(item)

    def put_nowait(self, item):
        self._q.put_nowait(item)

    async def get(self):
        return await self._q.get()

    def empty(self):
        return self._q.empty()
