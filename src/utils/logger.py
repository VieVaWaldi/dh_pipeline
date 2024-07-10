import logging
import os

from utils.file_handling import ensure_path

LOG_LEVEL = logging.DEBUG


class CustomFormatter(logging.Formatter):
    """
    Custom formatter for logging to
    - Pad all columns to a fixed width
    """

    def format(self, record):
        record.levelname = f"{record.levelname:<5}"
        record.filename = f"{record.filename:<17}"
        return super().format(record)


def setup_logging(log_dir: str) -> None:
    """
    Sets up and configures the logging module.
    Call once at the beginning of each run.
    """
    ensure_path(log_dir)

    logger = logging.getLogger()

    if logger.hasHandlers():
        return

    logger.setLevel(LOG_LEVEL)

    file_handler = logging.FileHandler(os.path.join(log_dir, "log_source.log"))
    console_handler = logging.StreamHandler()

    file_handler.setLevel(LOG_LEVEL)
    console_handler.setLevel(LOG_LEVEL)

    formatter = CustomFormatter(
        "[%(levelname)s] [%(asctime)s] [%(filename)-15s:%(lineno)-4d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def log_and_raise_exception(err_msg: str):
    """
    Logs the message and raises a standard exception.
    """
    logging.exception(err_msg)
    raise Exception(err_msg)
