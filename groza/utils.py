import logging
import logging.handlers
from abc import abstractmethod

from datetime import datetime, date, timezone
from uuid import UUID


def json_serial(obj):
    if isinstance(obj, datetime):
        serial = obj.replace(tzinfo=timezone.utc).timestamp()
        return int(serial)

    if isinstance(obj, date):
        DAY = 24 * 60 * 60  # POSIX day in seconds (exact value)
        timestamp = (obj - date(1970, 1, 1)).days * DAY
        return timestamp

    if isinstance(obj, UUID):
        serial = str(obj)
        return serial

    raise TypeError("Type %s not serializable" % type(obj))


def init_file_loggers(filename, names):
    for name in names:
        logger = logging.getLogger(name)

        file_handler = logging.handlers.RotatingFileHandler(filename, encoding="utf-8", maxBytes=16 * 1024 * 1024, backupCount=5)
        file_handler.setLevel(level=logger.level)

        formatter = logging.Formatter("[%(name)s]: %(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)


loggers = {}
def build_logger(name, is_debug=True):
    logger = loggers.get(name, None)
    if logger is not None:
        return logger

    level = logging.INFO
    if is_debug:
        level = logging.DEBUG

    formatter = logging.Formatter("[%(name)s]: %(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level=level)
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level=level)
    logger.addHandler(stream_handler)

    loggers[name] = logger
    return logger


class FieldTransformer:
    @abstractmethod
    def to_db(self, name: str) -> str:
        return name

    @abstractmethod
    def from_db(self, name: str) -> str:
        return name


class CamelCaseFieldTransformer(FieldTransformer):
    def to_db(self, name: str) -> str:
        """
        camelCase -> camel_case
        """
        build = ""
        for c in name:
            if c.isupper():
                build += "_" + c.lower()
            else:
                build += c

        return build

    def from_db(self, name: str) -> str:
        """
        underscore_case -> underscoreCase
        """
        build = ""

        ST_NORMAL = 0
        ST_LOWER = 1

        state = ST_NORMAL

        for c in name:
            if state == ST_NORMAL:
                if c == "_":
                    state = ST_LOWER
                else:
                    build += c
            elif state == ST_LOWER:
                build += c.upper()
                state = ST_NORMAL

        return build
