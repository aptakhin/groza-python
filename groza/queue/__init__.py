from abc import abstractmethod, ABC


class BaseQueue(ABC):
    def __init__(self):
        pass

    @abstractmethod
    async def put(self, item):
        pass

    @abstractmethod
    def put_nowait(self, item):
        pass

    @abstractmethod
    async def get(self):
        pass

    @abstractmethod
    def empty(self):
        pass
