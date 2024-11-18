import logging
from abc import ABC, abstractmethod
from pathlib import Path

from common_utils.config.config_loader import get_config
from common_utils.file_handling.file_handling import (
    ensure_path_exists,
    get_root_path,
    raise_error_if_directory_does_not_exists,
)
from common_utils.logging.logger import setup_logging
from extractors.extractor_interface import clean_extractor_name


class IAnalysisJob(ABC):
    """
    Abstract Class all analysis jobs have to inherit from.
    Sets up all paths and logging, enforces run and saving output.
    """

    def __init__(self, analysis_name: str, query_name: str):
        config = get_config()
        self.analysis_name = analysis_name

        self.output_path: Path = (
            get_root_path()
            / config["analysis_path"]
            / self.analysis_name
            / clean_extractor_name(query_name)
        )
        ensure_path_exists(self.output_path)

        if config["data_path"].startswith("/"):
            base_data_path = Path(config["data_path"])
        else:
            base_data_path = get_root_path() / config["data_path"]
        self.data_path = base_data_path / "extractors" / query_name
        raise_error_if_directory_does_not_exists(self.data_path)

        logging_path: Path = (
            get_root_path() / config["logging_path"] / "analysis" / self.analysis_name
        )
        ensure_path_exists(logging_path)
        setup_logging(logging_path, "analysis")

        logging.info(f"\n>>> Starting new analysis: {self.analysis_name}")

    @abstractmethod
    def run(self) -> None:
        """
        ...
        """

    @abstractmethod
    def save_output(self) -> None:
        """
        ...
        """
