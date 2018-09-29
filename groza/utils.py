import logging
import logging.handlers

from datetime import datetime, date


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
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

