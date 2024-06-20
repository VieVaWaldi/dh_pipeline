import os
import time
import logging
import zipfile

from datetime import datetime, timedelta
from dotenv import load_dotenv


from extractors.i_extractor import IExtractor
from utils.requests import make_request, download_file

load_dotenv()


class CordisExtractor(IExtractor):
    def extract_until_next_checkpoint(self, query):
        """
        Cordis extractions must follow this pattern:
        1. getExtraction
        2. Monitor getExtractionStatus until data is ready to be downloaded.
        3. Download the data
        4. Store the checkpoint
        5. deleteExtraction
        """
        api_key = os.getenv("API_KEY_CORDIS")
        last_response = self.cordis_get_extraction(api_key, query)

        if last_response["payload"].get("error"):
            err_msg = f"Response error: {last_response['payload']['error']}"
            logging.error(err_msg)
            raise Exception(err_msg)

        task_id = last_response["payload"]["taskID"]
        start_time = datetime.now()

        while True:
            if datetime.now() - start_time >= timedelta(hours=12):
                err_msg = "Error: Aborted because 12 hours passed since request start."
                logging.error(err_msg)
                raise Exception(err_msg)

            last_response = self.cordis_get_extraction_status(api_key, task_id)

            if last_response["payload"]["progress"] == "Finished":
                logging.info("Finished request")
                break

            time.sleep(60)

        base_url = "https://cordis.europa.eu/"
        download_url = base_url + last_response["payload"]["destinationFileUri"]
        self.store_extracted_data(download_url)

        self.store_new_checkpoint()

        self.cordis_delete_extraction(api_key, task_id)

    def store_extracted_data(self, source):
        """
        CORDIS data is returned as a ZIP and must be extracted that way.
        """
        zip_path = download_file(source)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(self.save_data_path)

        logging.info(f"Data saved to {self.save_path}.")

        # os.remove(zip_path)
        logging.error(f"REMOVE ZIP {self.save_path}.")

    def store_new_checkpoint(self):
        logging.error("Not implemented yet")
        pass

    def cordis_get_extraction(self, key, query):
        base_url = "https://cordis.europa.eu/api/dataextractions/getExtraction"
        params = {
            "query": query,
            "outputFormat": "xml",
            "key": key,
            "archived": True,
        }
        return make_request(base_url, params)

    def cordis_get_extraction_status(self, key, task_id):
        base_url = "https://cordis.europa.eu/api/dataextractions/getExtractionStatus"
        params = {"key": key, "taskId": task_id}
        return make_request(base_url, params)

    def cordis_delete_extraction(self, key, task_id):
        base_url = "https://cordis.europa.eu/api/dataextractions/deleteExtraction"
        params = {"key": key, "taskId": task_id}
        return make_request(base_url, params)


if __name__ == "__main__":
    extractor_name = "cordis_TEST"
    extractor = CordisExtractor(extractor_name)

    # query = "('cultural' AND 'heritage')"
    query = "cultural AND heritage AND computing AND print"
    extractor.extract_until_next_checkpoint(query)
