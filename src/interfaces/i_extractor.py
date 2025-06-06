from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from lib.file_handling.file_utils import (
    ensure_path_exists,
    write_file,
    load_file,
)
from lib.file_handling.path_utils import get_source_data_path, get_project_root_path
from utils.config.config_loader import get_config


@dataclass
class ExtractorConfig:
    name: str
    query: str
    query_id: int
    checkpoint_name: str
    checkpoint_start: str
    checkpoint_range: str
    download_attachments: bool


class IExtractor(ABC):
    """
    Base Extractor class that sets up paths and checkpoint.
    Enforces
    - extract_until_next_checkpoint(self) -> bool
    -
    """

    def __init__(self, extractor_config: ExtractorConfig):
        self.extractor_name: str = (
            f"{extractor_config.name}-query_id-{extractor_config.query_id}"
        )
        self.query: str = extractor_config.query
        self.download_attachments: bool = extractor_config.download_attachments

        self.checkpoint_name: str = extractor_config.checkpoint_name
        self.checkpoint_start: str = extractor_config.checkpoint_start
        self.checkpoint_range: str = extractor_config.checkpoint_range
        self.checkpoint_path = self._get_checkpoint_path()
        ensure_path_exists(self.checkpoint_path)
        self.checkpoint = self.restore_checkpoint()

        self.data_path = get_source_data_path(
            extractor_config.name, extractor_config.query_id
        )
        ensure_path_exists(self.data_path)

    def _get_checkpoint_path(self) -> Path:
        return Path(
            get_project_root_path()
            / get_config()["checkpoint_path"]
            / "extractor"
            / self.extractor_name
            / f"{self.checkpoint_name}.cp"
        )

    @abstractmethod
    def extract_until_checkpoint_range(self) -> bool:
        """Extract until checkpoint end; return whether to continue extraction."""

    @abstractmethod
    def restore_checkpoint(self) -> str:
        """
        Loads the checkpoint from the checkpoint file and returns it.
        Returns self.checkpoint_start when there is no checkpoint yet.
        """
        checkpoint = load_file(self.checkpoint_path)
        return checkpoint if checkpoint is not None else self.checkpoint_start

    def save_checkpoint(self, new_checkpoint: str):
        """
        Overwrites the checkpoint file with the latest checkpoint.
        """
        return write_file(self.checkpoint_path, new_checkpoint)

    # @abstractmethod
    # def create_checkpoint_range_for_this_run(self, next_checkpoint: str) -> str:
    #     """
    #     Returns the maximum check point until this extraction should run.
    #     """
    #
    #
    # @abstractmethod
    # def save_extracted_data(self, data: Union[str, Dict[str, Any]]) -> Path:
    #     """
    #     Once the extraction is done, save all the data.
    #     Returns path to saved data.
    #     """
    #
    # @abstractmethod
    # def non_contextual_transformation(self, data_path: Path) -> None:
    #     """
    #     1. Create a directory for each record dataset
    #     2. Whitespace trimming
    #     3. Character encoding normalization
    #     4. Optionally find, download and save all attached links
    #     """
    #
    # @abstractmethod
    # def get_new_checkpoint_from_data(self) -> Any:
    #     """
    #     Once the extraction is done, retrieve the checkpoint and save it using
    #     :func:`save_checkpoint`.
    #     Returns the extracted checkpoint.
    #     """
