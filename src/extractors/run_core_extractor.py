import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv

from extractors.i_extractor import IExtractor
from utils.config_loader import get_query_config
from utils.logger import log_and_raise_exception
from utils.web_requests import make_get_request


class CoreExtractor(IExtractor):
    """
    A Core extraction must follow this pattern:
    1. Call search which also returns the data (great)
    However core has too many papers. One results returns more than the api allows,
    so we limit it to 10k, remember the checkpoint and start the next query
    sorted from there.
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        super().__init__(extractor_name, checkpoint_name)
        self.api_key = os.getenv("API_KEY_CORE")
        self.base_url = "https://api.core.ac.uk/v3"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def extract_until_next_checkpoint(self, query: str) -> None:
        if not self.api_key:
            return log_and_raise_exception("API Key not found")

        core_data = self._search_core(query)

        data_path = self.save_extracted_data(core_data)
        # self.non_contextual_transformation(data_path)

        # checkpoint = self.get_new_checkpoint()
        # self.save_checkpoint(checkpoint)

        logging.info(">>> Successfully finished extraction")

    def restore_checkpoint(self) -> str:
        # checkpoint = load_file(self.checkpoint_path)
        # return checkpoint if checkpoint is not None else "1990-01-01"
        pass

    def create_next_checkpoint_end(self, next_checkpoint: str) -> str:
        # 2016-06-16T12:26:00

        # last_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
        # try:
        #     new_date = last_date.replace(year=last_date.year + int(next_checkpoint))
        # except ValueError:
        #     new_date = last_date.replace(  # Handles February 29th for leap years
        #         year=last_date.year + int(next_checkpoint), day=28
        #     )
        # return new_date.strftime("%Y-%m-%d")
        return "1995-01-01"

    def save_extracted_data(self, data: List[Dict[str, Any]]) -> Path:
        for index, entry in enumerate(data):
            filename = f"entry_{index}.json"
            file_path = os.path.join(self.data_path, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                json_string = json.dumps(entry, indent=4, ensure_ascii=False)
                f.write(json_string)

        logging.info(
            f"Saved {len(data)} entries as individual files in {self.data_path}",
        )
        return Path("")

    def non_contextual_transformation(self, data_path: str):
        pass

    def get_new_checkpoint(self) -> str:
        # return "1995-01-01"
        pass

    def _search_core(self, query: str, limit: int = 2) -> (List[Dict[str, Any]], bool):
        current_offset = self.restore_checkpoint()
        params = {
            "q": query,
            "sort": "acceptedDate",
            "limit": limit,
            "offset": current_offset,
        }
        response = make_get_request(
            f"{self.base_url}/search/works", params, self.headers
        )

        # 'totalHits' = {int} 740089
        # 'limit' = {int} 2
        # 'offset' = {int} 0

        idk = (current_offset + 1) * limit
        b = idk >= int(response["totalHits"])

        return response["results"], b


def start_extraction(query: str, extractor_name: str, checkpoint_name: str):
    extractor = CoreExtractor(
        extractor_name=extractor_name, checkpoint_name=checkpoint_name
    )

    extractor.extract_until_next_checkpoint(query)


def main():
    load_dotenv()
    config = get_query_config()["core"]

    # Limit 3k
    # Sort by acceptedDate
    # Checkpoint offset, if data got is below limit don't remember offset +=1
    # Simply update the existing data for same offset on next run

    query = config["queries"][0]
    extractor_name = f"core_{query.replace(' ', '')}"
    checkpoint_name = config["checkpoint"]

    # IF NO (or base) CHECKPOINT RUN IN THIS LOOP TO CATCH UP till today
    for i in range(1):
        start_extraction(query, extractor_name, checkpoint_name)

    # IF HAS CHECKPOINT JUST RUN ONCE FROM IT TO GET NEW DATA SINCE THEN


if __name__ == "__main__":
    main()
