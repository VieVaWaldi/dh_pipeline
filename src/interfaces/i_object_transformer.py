from abc import ABC, abstractmethod
from typing import Any, Tuple


class IObjectTransformer(ABC):

    def __init__(self):  # , transformer_name: str
        # config = get_config()
        #
        # self.logging_path: Path = (
        #     get_root_path() / config["logging_path"] / "extractors" / transformer_name
        # )
        # ensure_path_exists(self.logging_path)

        # setup_logging(self.logging_path, "extractor")
        # logger.info(
        #     "\n>>> Starting new data extraction run for %s from checkpoint %s.",
        #     transformer_name,
        #     (self.last_checkpoint if self.last_checkpoint else "No Checkpoint"),
        # )
        pass

    @abstractmethod
    def transform(self, data: Any) -> Tuple[Any, bool]:
        """
        Use this method to transform raw data from the extractor
        to a data object.

        Returns:
            The Top Level Data object
            True if the object should be ingested into the database
        """
