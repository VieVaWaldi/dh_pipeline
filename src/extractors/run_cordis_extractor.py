import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

from extractors.i_extractor import IExtractor
from utils.config_loader import get_query_config
from utils.file_handling import unpack_and_remove_zip
from utils.logger import log_and_raise_exception
from utils.requests import make_delete_request, make_get_request, download_file
from utils.xml_parser import XMLCheckpointParser


class CordisExtractor(IExtractor):
    """
    A Cordis extraction must follow this pattern:
    1. call getExtraction
    2. Monitor getExtractionStatus until data is ready to be downloaded
    3. Download the data
    4. Store the checkpoint
    5. deleteExtraction
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        super().__init__(extractor_name, checkpoint_name)
        self.parser = XMLCheckpointParser(checkpoint_name)
        self.base_checkpoint = "1990-01-01"

    def create_next_checkpoint_end(self, next_checkpoint: str) -> str:
        if not self.last_checkpoint:
            self.last_checkpoint = self.base_checkpoint
        last_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
        try:
            new_date = last_date.replace(year=last_date.year + int(next_checkpoint))
        except ValueError:
            new_date = last_date.replace(  # Handles February 29th for leap years
                year=last_date.year + int(next_checkpoint), day=28
            )
        return new_date.strftime("%Y-%m-%d")

    def extract_until_next_checkpoint(self, query: str) -> None:
        api_key = os.getenv("API_KEY_CORDIS")
        if not api_key:
            return log_and_raise_exception("API Key not found")

        task_id = self._cordis_get_extraction_task_id(api_key, query)
        download_uri = self._cordis_get_download_uri(api_key, task_id)
        self.save_extracted_data(download_uri)

        checkpoint = self.get_new_checkpoint()
        self.save_checkpoint(checkpoint)

        self._cordis_delete_extraction(api_key, task_id)

        logging.info(">>> Successfully finished extraction")

    def non_contextual_transformation(self):
        pass

    def save_extracted_data(self, data: str) -> None:
        base_url = "https://cordis.europa.eu/"
        zip_path = download_file(base_url + data, self.data_path)
        unpack_and_remove_zip(zip_path)

        # Cordis returns a zip in a fucking zip
        xml_zip_path = os.path.dirname(zip_path) + "/xml.zip"
        unpack_and_remove_zip(xml_zip_path)

    def get_new_checkpoint(self) -> str:
        checkpoint = self.parser.get_largest_checkpoint(self.data_path + "/xml")
        if not checkpoint:
            return log_and_raise_exception("Couldn't get checkpoint")
        return checkpoint

    def _cordis_get_extraction_task_id(self, key: str, query: str) -> str:
        """
        Calls Cordis getExtraction API and returns the task ID for the job.
        """
        base_url = "https://cordis.europa.eu/api/dataextractions/getExtraction"
        params = {
            "query": query,
            "outputFormat": "xml",
            "key": key,
            "archived": True,
        }

        response = make_get_request(base_url, params)
        if response["payload"].get("error"):
            log_and_raise_exception(f"Response error: {response['payload']['error']}")

        return response["payload"]["taskID"]

    def _cordis_get_download_uri(self, key: str, task_id: str) -> str:
        """
        Monitors the cordis getExtractionStatus API and returns the download uri
        once available. Returns an exception to running for longer than 12 hours.
        """
        base_url = "https://cordis.europa.eu/api/dataextractions/getExtractionStatus"
        params = {"key": key, "taskId": task_id}

        start_time = datetime.now()
        while True:

            response = make_get_request(base_url, params)
            if response["payload"]["progress"] == "Finished":
                return response["payload"]["destinationFileUri"]

            if response["payload"].get("error"):
                log_and_raise_exception(
                    f"Response error: {response['payload']['error']}"
                )

            if datetime.now() - start_time >= timedelta(hours=12):
                return log_and_raise_exception(
                    "Error: Aborted because 12 hours passed since request start."
                )

            time.sleep(60)

    def _cordis_delete_extraction(self, key: str, task_id: str):
        """
        Deletes the cordis extraction to make room for more.
        """
        base_url = "https://cordis.europa.eu/api/dataextractions/deleteExtraction"
        params = {"key": key, "taskId": task_id}
        make_delete_request(base_url, params)

        # if response["payload"].get("status") == "false":
        #     log_and_raise_exception(f"Response error: {response['payload']['status']}")


if __name__ == "__main__":
    load_dotenv()

    config = get_query_config()["cordis"]

    for i in range(3):
        CHECKPOINT = config["checkpoint"]
        EXTRACTOR_NAME = f"cordis_{config['queries'][0].replace(' ', '')}"
        extractor = CordisExtractor(
            extractor_name=EXTRACTOR_NAME, checkpoint_name=CHECKPOINT
        )

        CHECKPOINT_FROM = extractor.restore_checkpoint()
        if not CHECKPOINT_FROM:
            CHECKPOINT_FROM = extractor.base_checkpoint
        CHECKPOINT_TO = extractor.create_next_checkpoint_end("5")

        QUERY = f"{CHECKPOINT}={CHECKPOINT_FROM}-{CHECKPOINT_TO} AND "
        QUERY += "(cultural OR heritage)"

        extractor.extract_until_next_checkpoint(QUERY)
