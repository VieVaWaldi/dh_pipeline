import os
import logging
from datetime import datetime
from abc import ABC, abstractmethod

from utils.logger import setup_logging
from utils.file_handling import ensure_path


class IExtractor(ABC):
    """
    Abstract Class all exctractors have to inherit from.
    Sets up all import paths and logging.
    All extractors must follow this pattern:
    1. Get the data until next checkpoint
    2. Save the data
    3. Get the checkpoint
    4. Save the checkpoint
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        # Make this configurable from a file with dev and prod maybe
        self.checkpoint_path = f"./data/checkpoints/extractors/{extractor_name}/"
        self.checkpoint_file = f"CHK_{checkpoint_name}"
        ensure_path(self.checkpoint_path)
        self.last_checkpoint = self.restore_checkpoint()

        self.logging_path = f"./data/logs/extractors/{extractor_name}/"
        self.data_path = f"./data/pile/extractors/{extractor_name}/last_{checkpoint_name}_{self.last_checkpoint}/"
        ensure_path(self.logging_path)
        ensure_path(self.data_path)

        setup_logging(self.logging_path)

        logging.info(
            ">>> Starting NEW data extraction for %s from checkpoint %s.",
            extractor_name,
            self.last_checkpoint,
        )
        logging.error("ADD a config for paths with prod and dev.")

    def restore_checkpoint(self) -> str:
        """
        Loads the checkpoint from the checkpoint file and returns it.
        Returns 1990-01-01 when no checkpoint found.
        """
        checkpoint_base = "1990-01-01"
        if os.path.exists(self.checkpoint_path + self.checkpoint_file):
            with open(self.checkpoint_path + self.checkpoint_file, "r+") as file:
                checkpoint = file.read().strip()
                return checkpoint if checkpoint else checkpoint_base
        return checkpoint_base

    def save_checkpoint(self, new_checkpoint: str) -> None:
        """
        Overwrites the checkpoint file with the latest checkpoint.
        """
        with open(self.checkpoint_path + self.checkpoint_file, "w+") as file:
            file.write(new_checkpoint)

    def get_max_checkpoint_for_this_run(self, years: int) -> str:
        """
        Returns the maximum point until this extraction should run.
        """
        last_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
        try:
            new_date = last_date.replace(year=last_date.year + years)
        except ValueError:
            # Handles February 29th for leap years
            new_date = last_date.replace(year=last_date.year + years, day=28)
        return new_date.strftime("%Y-%m-%d")

    @abstractmethod
    def extract_until_next_checkpoint(self, query: str) -> None:
        """
        Is responsible for extracting, downloading, saving the data and cleaning up.
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
