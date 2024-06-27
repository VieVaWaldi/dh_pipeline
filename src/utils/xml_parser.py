import os
import logging
import xml.etree.ElementTree as ET
from typing import List, Tuple, Optional

from utils.logger import log_and_raise_exception


class XMLCheckpointParser:
    """
    Parses XML files in a directory path and extracts specified checkpoints.
    """

    def __init__(self, checkpoint: str):
        self.checkpoint = checkpoint

    def parse_files(self, directory_path: str) -> List[Tuple[str, Optional[str]]]:
        """
        Parses all XML files in the directory path and extracts the checkpoint data.
        """
        results = []
        for filename in os.listdir(directory_path):
            if filename.endswith(".xml"):
                file_path = os.path.join(directory_path, filename)
                result = self.parse_file(file_path)
                if result:
                    results.append(result)
        return results

    def parse_file(self, file_path: str) -> Optional[Tuple[str, Optional[str]]]:
        """
        Parses a single XML file and extracts the checkpoint data.
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            checkpoints = root.findall(f".//{self.checkpoint}")

            if len(checkpoints) > 2:
                print(
                    f"More than 2 {self.checkpoint} elements found in file: {file_path}"
                )
            elif len(checkpoints) == 0:
                print(f"No {self.checkpoint} elements found in file: {file_path}")
                return None

            # Assuming we only care about the first checkpoint element if there are multiple
            first_checkpoint = checkpoints[0].text if checkpoints else None
            return (os.path.basename(file_path), first_checkpoint)

        except ET.ParseError as e:
            return log_and_raise_exception(f"Error parsing XML file {file_path}: {e}")

    def get_largest_checkpoint(self, directory_path: str) -> str | None:
        """
        Returns the largest checkpoint value.
        """
        checkpoint_list = self.parse_files(directory_path)
        sorted_results = sorted(checkpoint_list, key=lambda x: x[1] or "", reverse=True)
        largest_checkpoint = sorted_results[0] if sorted_results else None
        if not largest_checkpoint:
            return None
        return largest_checkpoint[1]


if __name__ == "__main__":
    parser = XMLCheckpointParser("startDate")
    DIRECTORY_PATH = "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/cordis_TEST/last_startDate_1990-01-01/xml"
    print(parser.get_largest_checkpoint(DIRECTORY_PATH))
