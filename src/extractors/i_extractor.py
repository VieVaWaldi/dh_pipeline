import os
from abc import ABC, abstractmethod
import logging

from utils.file_handling import ensure_path


class IExtractor(ABC):
    def __init__(self, extractor_name):
        self.logging_path = f"logs/extractors/{extractor_name}/"
        logging.error("CHANGE THIS FOR DRACO! or simply a config.")
        # AUCH HIER ein ordner pro checkpoint run ...
        self.save_data_path = f"./data/extractors/{extractor_name}/"
        self.checkpoint_path = f"./checkpoints/extractors/{extractor_name}/"

        ensure_path(self.logging_path)
        ensure_path(self.save_data_path)
        ensure_path(self.checkpoint_path)

        self.last_checkpoint = self.load_checkpoint()

        self.setup_logging()
        logging.info(
            f"Starting data extraction for {extractor_name} from checkpoint {self.load_checkpoint()}.",
            format="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        )

    def setup_logging(self):
        logging.basicConfig(
            filename=os.path.join(self.logging_path, "data_source.log"),
            level=logging.INFO,
        )

    def load_checkpoint(self):
        # if os.path.exists(self.checkpoint_path):
        #     with open(self.checkpoint_path, "r") as f:
        #         pass
        #         # return json.load(f)
        # else:
        return None

    def save_checkpoint(self, checkpoint):
        # with open(self.checkpoint_path, "w") as f:
        #     pass
        # json.dump(checkpoint, f)
        logging.info(f"Checkpoint updated at {self.checkpoint_path}.")

    @abstractmethod
    def extract_until_next_checkpoint(self):
        """
        Is responsible for extracting, downloading, saving the data and cleaning up.
        """
        pass

    @abstractmethod
    def store_extracted_data(self, source):
        """
        Once the extraction is done, store all the data.
        """
        pass

    @abstractmethod
    def store_new_checkpoint(self):
        """
        Once the extraction is done, store and return the new checkpoint.
        """
        pass
