import logging
import os


def get_log_level(name: str = None) -> str:
    if name is None:
        level_name = "INFO"
    else:
        name = name.upper()
        name = "DPK_" + name + "_LOG_LEVEL"
        level_name = os.environ.get(name, "INFO")
    return level_name


__logger_cache = {}


def get_logger(name: str, level=None, file=None) -> logging.Logger:
    logger = __logger_cache.get(name, None)
    if logger is not None:
        return logger
    logger = logging.getLogger(name)
    if level is None:
        level = get_log_level(name)
    logger.setLevel(level)
    c_handler = logging.StreamHandler()
    if level == "DEBUG":
        # When debugging, include the source link that pycharm understands.
        msgfmt = '%(asctime)s %(levelname)s - %(message)s at "%(pathname)s:%(lineno)d"'
    else:
        msgfmt = "%(asctime)s %(levelname)s - %(message)s"
    timefmt = "%H:%M:%S"

    c_format = logging.Formatter(msgfmt, timefmt)
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    if file is not None:
        f_handler = logging.FileHandler(file)
        f_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

    # Add handlers to the logger
    __logger_cache[name] = logger
    return logger
