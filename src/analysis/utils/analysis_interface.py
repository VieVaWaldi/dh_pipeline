import logging
from abc import ABC
from pathlib import Path

from common_utils.config.config_loader import get_config
from common_utils.logger.logger import setup_logging
from core.extractor.utils import clean_extractor_name
from core.file_handling.file_utils import (
    get_project_root_path,
    ensure_path_exists,
    raise_error_if_directory_does_not_exists,
)


class IAnalysisJob(ABC):
    """
    Abstract Class all analysis jobs have to inherit from.
    Sets up all paths and logger, enforces run and saving output.
    """

    def __init__(self, analysis_name: str, query_name: str):
        config = get_config()
        self.analysis_name = analysis_name

        """ Output File """
        self.analysis_output_path: Path = (
                get_project_root_path()
                / config["analysis_path"]
                / self.analysis_name
                / clean_extractor_name(query_name)
        )
        ensure_path_exists(self.analysis_output_path)

        """ Data Path """
        if config["data_path"].startswith("/"):
            base_data_path = Path(config["data_path"])
        else:
            base_data_path = get_project_root_path() / config["data_path"]
        self.data_path = base_data_path / query_name
        raise_error_if_directory_does_not_exists(self.data_path)

        """ Logging """
        logging_path: Path = (
                get_project_root_path() / config["logging_path"] / "analysis" / self.analysis_name
        )
        ensure_path_exists(logging_path)
        setup_logging(logging_path, "analysis")

        logging.info(f"\n>>> Starting new analysis: {self.analysis_name}")

    # @abstractmethod
    # def run(self) -> None:
    #     """
    #     ...
    #     """
    #
    # @abstractmethod
    # def save_output(self) -> None:
    #     """
    #     ...
    #     """
