import logging
from abc import ABC, abstractmethod

from utils.config_loader import get_config
from utils.file_handling import ensure_path, load_file, write_file
from utils.logger import setup_logging


class IExtractor(ABC):
    """
    Abstract Class all extractors have to inherit from.
    Sets up all paths and logging.
    All extractors must follow this pattern:
    1. Get the data until next checkpoint
    2. Save the data
    3. Get the checkpoint
    4. Save the checkpoint
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        config = get_config()

        self.checkpoint_path = (
            f"{config['checkpoint_path']}extractors/{extractor_name}/"
        )
        self.checkpoint_file = f"CP_{checkpoint_name}"
        ensure_path(self.checkpoint_path)

        self.last_checkpoint = self.restore_checkpoint()

        self.data_path = (
            f"{config['data_path']}extractors/{extractor_name}/"
            f"last_{checkpoint_name}_{self.last_checkpoint}/"
        )
        ensure_path(self.data_path)

        self.logging_path = f"{config['logging_path']}extractors/{extractor_name}/"
        ensure_path(self.logging_path)
        setup_logging(self.logging_path)
        logging.info(
            ">>> Starting new data extraction run for %s from checkpoint %s.",
            extractor_name,
            (self.last_checkpoint if not self.last_checkpoint else "No Checkpoint"),
        )

    def restore_checkpoint(self) -> str | None:
        """
        Loads the checkpoint from the checkpoint file and returns it.
        Returns None when there is no file.
        """
        return load_file(self.checkpoint_path + self.checkpoint_file)

    def save_checkpoint(self, new_checkpoint: str) -> None:
        """
        Overwrites the checkpoint file with the latest checkpoint.
        """
        return write_file(self.checkpoint_path + self.checkpoint_file, new_checkpoint)

    @abstractmethod
    def create_next_checkpoint_end(self, next_checkpoint: str) -> str:
        """
        Returns the maximum check point until this extraction should run.
        """

    @abstractmethod
    def extract_until_next_checkpoint(self, query: str) -> None:
        """
        Is responsible for extracting, downloading, non-contextual transforming,
        saving the data and cleaning up.
        """

    @abstractmethod
    def non_contextual_transformation(self) -> None:
        """
        1. Find and download all attached links and save them
        2. Whitespace trimming
        3. Character encoding normalization
        """

    @abstractmethod
    def save_extracted_data(self, data: str) -> None:
        """
        Once the extraction is done, save all the data.
        """

    @abstractmethod
    def get_new_checkpoint(self) -> str:
        """
        Once the extraction is done, retrieve the checkpoint and save it using
        :func:`save_checkpoint`. Returns the extracted checkpoint.
        """
