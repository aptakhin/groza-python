from abc import abstractmethod

from groza import GrozaUser, GrozaRequest


class BaseAuth:
    @abstractmethod
    def register(self, request: GrozaRequest) -> GrozaUser:
        pass

    @abstractmethod
    def login(self, request: GrozaRequest) -> GrozaUser:
        pass
