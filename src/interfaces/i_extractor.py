import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union

from core.etl.extractor.utils import clean_extractor_name
from core.file_handling.file_handling import (
    ensure_path_exists,
    get_root_path,
    write_file,
)
from common_utils.config.config_loader import get_config
from common_utils.logger.logger import setup_logging


class IExtractor(ABC):
    """
    Abstract Class all extractors have to inherit from.
    Sets up all paths and logger.
    All extractors must follow this pattern:
    1. Create the end checkpoint for this run.
    2. Extract data until next checkpoint.
    3. Save the data.
    4. Non contextually transform the data @see docs for more info.
    5. Get the new checkpoint from the latest data point.
    6. Save the checkpoint.
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        config = get_config()

        extractor_name = clean_extractor_name(extractor_name)
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

        if config["data_path"].startswith("/"):
            base_data_path = Path(config["data_path"])
        else:
            base_data_path = get_root_path() / config["data_path"]

        self.data_path = (
            base_data_path
            # / "extractors"
            / extractor_name
            / f"last_{checkpoint_name}_{self.last_checkpoint}/"
        )
        ensure_path_exists(self.data_path)

        self.logging_path: Path = (
            get_root_path() / config["logging_path"] / "extractors" / extractor_name
        )
        ensure_path_exists(self.logging_path)

        setup_logging(self.logging_path, "extractor")
        logging.info(
            "\n>>> Starting new data extraction run for %s from checkpoint %s.",
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
    def create_checkpoint_end_for_this_run(self, next_checkpoint: str) -> str:
        """
        Returns the maximum check point until this extraction should run.
        """

    @abstractmethod
    def extract_until_next_checkpoint(self, query: str) -> bool:
        """
        Is responsible for extracting, downloading, non-contextual transforming,
        saving the data and cleaning up.
        Returns true, when the run should continue, and false when there is no more data.
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
        4. Optionally find, download and save all attached links
        """

    @abstractmethod
    def get_new_checkpoint_from_data(self) -> Any:
        """
        Once the extraction is done, retrieve the checkpoint and save it using
        :func:`save_checkpoint`.
        Returns the extracted checkpoint.
        """
