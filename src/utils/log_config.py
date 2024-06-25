import logging
import os

from utils.file_handling import ensure_path


def setup_logging(log_dir):
    ensure_path(log_dir)

    logger = logging.getLogger()

    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(os.path.join(log_dir, "data_source.log"))
        console_handler = logging.StreamHandler()

        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "[%(levelname)s] [%(asctime)s] [%(filename)s] %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
