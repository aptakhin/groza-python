

class User:
    def __init__(self, auth_token=None, user_id=None):
        self.auth_token = auth_token
        self.user_id = user_id


class Table:
    def __init__(self, primary_key):
        self.primary_key = primary_key

        self.before_update = {}


