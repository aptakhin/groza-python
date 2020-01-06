from groza import GrozaRequest
from groza.auth import BaseAuth, GrozaUser


class DebugAuth(BaseAuth):
    def register(self, request: GrozaRequest) -> GrozaUser:
        pass

    def login(self, request: GrozaRequest) -> GrozaUser:
        return GrozaUser(user_id=1)
