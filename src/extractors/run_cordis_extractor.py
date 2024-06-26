import logging
import os
import time

from datetime import datetime, timedelta
from venv import logger
from dotenv import load_dotenv


from extractors.i_extractor import IExtractor
from utils.file_handling import unpack_and_remove_zip
from utils.logger import log_and_raise_exception
from utils.requests import make_delete_request, make_get_request, download_file


class CordisExtractor(IExtractor):
    """
    A Cordis extraction must follow this pattern:
    1. call getExtraction
    2. Monitor getExtractionStatus until data is ready to be downloaded
    3. Download the data
    4. Store the checkpoint
    5. deleteExtraction
    """

    def extract_until_next_checkpoint(self, query: str) -> None:

        # api_key = os.getenv("API_KEY_CORDIS")
        # if not api_key:
        #     return log_and_raise_exception("API Key not found")

        # task_id = self.cordis_get_extraction_task_id(api_key, query)

        # download_uri = self.cordis_get_download_uri(api_key, task_id)
        # self.save_extracted_data(download_uri)

        # checkpoint = self.get_new_checkpoint()
        # self.save_checkpoint(checkpoint)

        # self.cordis_delete_extraction(api_key, task_id)

        # self.get_new_checkpoint()

        logging.info(">>> Successfully finished extraction")

    def save_extracted_data(self, url: str) -> None:
        base_url = "https://cordis.europa.eu/"
        zip_path = download_file(base_url + url, self.data_path)
        unpack_and_remove_zip(zip_path)

        # Cordis returns a zip in a fucking zip
        xml_zip_path = os.path.dirname(zip_path) + "/xml.zip"
        unpack_and_remove_zip(xml_zip_path)

    def get_new_checkpoint(self) -> str:
        pass

    def cordis_get_extraction_task_id(self, key: str, query: str) -> str:
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

    def cordis_get_download_uri(self, key: str, task_id: str) -> str:
        """
        Monitors the cordis getExtractionStatus API and returns the download uri
        once available. Returns an exception of running for longer than 12 hours.
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

    def cordis_delete_extraction(self, key: str, task_id: str):
        """
        Deletes the cordis extraction to make room for more.
        """
        base_url = "https://cordis.europa.eu/api/dataextractions/deleteExtraction"
        params = {"key": key, "taskId": task_id}
        response = make_delete_request(base_url, params)

        logger.error("LOG the success of deletion, i mean i did kinda with 200...")
        # if response["payload"].get("status") == "false":
        #     log_and_raise_exception(f"Response error: {response['payload']['status']}")


if __name__ == "__main__":
    load_dotenv()

    extractor = CordisExtractor("cordis_TEST")

    # query = "('cultural' AND 'heritage')"
    QUERY = "ice AND print AND computing AND sea AND future AND rise"
    extractor.extract_until_next_checkpoint(QUERY)
