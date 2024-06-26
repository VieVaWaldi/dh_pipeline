import logging
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

    def __init__(self, extractor_name: str):
        # Add /{checkpoint} and makes this configurable from a file with dev and prod maybe
        self.logging_path = f"logs/extractors/{extractor_name}/"
        self.data_path = f"./data/extractors/{extractor_name}/"
        self.checkpoint_path = f"./checkpoints/extractors/{extractor_name}/"

        ensure_path(self.logging_path)
        ensure_path(self.data_path)
        ensure_path(self.checkpoint_path)

        setup_logging(self.logging_path)

        self.last_checkpoint = self.restore_checkpoint()

        logging.info(
            ">>> Starting NEW data extraction for %s from checkpoint %s.",
            extractor_name,
            self.last_checkpoint,
        )
        logging.error(
            "ADD CHECKPOINT TO paths and CHANGE PATHS FOR DRACO! or simply add a config with prod and dev."
        )

    def restore_checkpoint(self) -> str:
        """
        Loads the checkpoint from the checkpoint file and returns it.
        """
        # if os.path.exists(self.checkpoint_path):
        #     with open(self.checkpoint_path, "r") as f:
        #         pass
        #         # return json.load(f)
        # else:
        return None

    def save_checkpoint(self, checkpoint: str) -> None:
        """
        Saves the checkpoint to the checkpoint file.
        """
        # with open(self.checkpoint_path, "w") as f:
        #     pass
        # json.dump(checkpoint, f)
        logging.info("Checkpoint updated at %s.", self.checkpoint_path)

    @abstractmethod
    def extract_until_next_checkpoint(self, query: str) -> None:
        """
        Is responsible for extracting, downloading, saving the data and cleaning up.
        """

    @abstractmethod
    def save_extracted_data(self, url: str) -> None:
        """
        Once the extraction is done, save all the data.
        """

    @abstractmethod
    def get_new_checkpoint(self) -> str:
        """
        Once the extraction is done, retrieve the checkpoint and save it using
        :func:`save_checkpoint`. Returns the extracted checkpoint.
        """
