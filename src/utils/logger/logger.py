import logging
from pathlib import Path

from lib.file_handling.file_utils import ensure_path_exists

LOG_LEVEL = logging.INFO


class CustomFormatter(logging.Formatter):
    """
    Custom formatter for logger to
    - Pad all columns to a fixed width
    """

    def format(self, record):
        record.levelname = f"{record.levelname:<5}"
        record.filename = f"{record.filename:<17}"
        return super().format(record)


def setup_logging(log_path: Path, log_name: str) -> None:
    """
    Sets up and configures the logger module.
    Call once at the beginning of each run.
    """
    ensure_path_exists(log_path)

    logger = logging.getLogger()

    if logger.hasHandlers():
        return

    logger.setLevel(LOG_LEVEL)

    file_handler = logging.FileHandler(log_path / f"{log_name}.log")
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
