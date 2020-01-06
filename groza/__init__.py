import contextvars


class GrozaUser:
    def __init__(self, auth_token=None, user_id=None):
        self.auth_token = auth_token
        self.user_id = user_id


class GrozaRequest:
    def __init__(self, data):
        self._data = data


class GrozaResponse:
    def __init__(self, data, request=None):
        self._data = data
        self._request = request

    @property
    def data(self):
        return self._data

