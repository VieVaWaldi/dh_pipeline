import json
import logging
import os
import time
from typing import List, Dict, Any

import requests

from extractors.i_extractor import IExtractor
from utils.logger import log_and_raise_exception


class COREExtractor(IExtractor):
    """
    WIP!
    Checkpoint are missing.
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        super().__init__(extractor_name, checkpoint_name)
        self.api_key = os.getenv("API_KEY_CORE")
        self.base_url = "https://api.core.ac.uk/v3"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def extract_until_next_checkpoint(self, query: str) -> None:
        if not self.api_key:
            return log_and_raise_exception("API Key not found")

        # start_date = "1990-01-01"
        # end_date = "2010-12-31"
        results = self.search_core()

        self.save_extracted_data(results)
        new_checkpoint = self.get_new_checkpoint()
        self.save_checkpoint(new_checkpoint)

        logging.info(">>> Successfully finished extraction")

    def search_core(
        self,
        limit: int = 1000,
        max_results: int = 100000,
    ) -> List[Dict[str, Any]]:
        query = "cultural heritage"
        results = []
        offset = 0

        while len(results) < max_results:
            try:
                response = requests.get(
                    f"{self.base_url}/search/works",
                    headers=self.headers,
                    params={
                        "q": query,
                        "limit": limit,
                        "offset": offset,
                        "sort": "publishedDate",
                    },
                    timeout=60,
                )
                response.raise_for_status()
                data = response.json()

                batch_results = data["results"]
                results.extend(batch_results)

                if len(batch_results) < limit:
                    break

                offset += limit
                logging.info("Fetched %s results", len(results))

                time.sleep(1)  # Respect rate limits

            except requests.RequestException as e:
                return log_and_raise_exception(
                    f"Error occurred: {e}\nReponse: {response.text}",
                )

        return results[:max_results]

    def save_extracted_data(self, data: List[Dict[str, Any]]) -> None:
        for index, entry in enumerate(data):
            filename = f"entry_{index}.json"
            file_path = os.path.join(self.data_path, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                json_string = json.dumps(entry, indent=4, ensure_ascii=False)
                f.write(json_string)

        logging.info(
            "Saved %s entries as individual files in %s", len(data), self.data_path
        )

    def get_new_checkpoint(self) -> str:
        # For now, just return the end date of our query range
        return "2000-12-31"


if __name__ == "__main__":
    extractor = COREExtractor(
        extractor_name="core_extractor", checkpoint_name="published_date"
    )
    extractor.extract_until_next_checkpoint("")
