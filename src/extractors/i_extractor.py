import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union

from utils.config_loader import get_config
from utils.file_handling import ensure_path_exists, write_file, get_root_path
from utils.logger import setup_logging


class IExtractor(ABC):
    """
    Abstract Class all extractors have to inherit from.
    Sets up all paths and logging.
    All extractors must follow this pattern:
    1. Create the end for the next checkpoint.
    2. Extract data until next checkpoint.
    3. Save the data.
    4. Non contextually transform the data.
    5. Get the checkpoint.
    6. Save the checkpoint.
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        config = get_config()

        self.checkpoint_name = checkpoint_name
        self.checkpoint_path: Path = (
            get_root_path()
            / config["checkpoint_path"]
            / "extractors"
            / extractor_name
            / f"{checkpoint_name}.cp"
        )
        ensure_path_exists(self.checkpoint_path)
        self.last_checkpoint: str = self.restore_checkpoint()

        self.data_path: Path = (
            get_root_path()
            / config["data_path"]
            / "extractors"
            / extractor_name
            / f"last_{checkpoint_name}_{self.last_checkpoint}/"
        )
        ensure_path_exists(self.data_path)

        self.logging_path: Path = (
            get_root_path() / config["logging_path"] / "extractors" / extractor_name
        )
        ensure_path_exists(self.logging_path)

        setup_logging(self.logging_path)
        logging.info(
            ">>> Starting new data extraction run for %s from checkpoint %s.",
            extractor_name,
            (self.last_checkpoint if self.last_checkpoint else "No Checkpoint"),
        )

    def save_checkpoint(self, new_checkpoint: str) -> None:
        """
        Overwrites the checkpoint file with the latest checkpoint.
        """
        return write_file(self.checkpoint_path, new_checkpoint)

    @abstractmethod
    def restore_checkpoint(self) -> str | None:
        """
        Load the checkpoint from the checkpoint file and returns it.
        Return BASE_CHECKPOINT when there is no file.
        """

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
    def save_extracted_data(self, data: Union[str, Dict[str, Any]]) -> Path:
        """
        Once the extraction is done, save all the data.
        Returns path to saved data.
        """

    @abstractmethod
    def non_contextual_transformation(self, data_path: Path) -> None:
        """
        1. Create a directory for each record dataset
        2. Whitespace trimming
        3. Character encoding normalization
        4. Find, download and save all attached links
        """

    @abstractmethod
    def get_new_checkpoint(self) -> str:
        """
        Once the extraction is done, retrieve the checkpoint and save it using
        :func:`save_checkpoint`.
        Returns the extracted checkpoint.
        """
