import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

import utils.xml_parser as xml
from extractors.i_extractor import IExtractor
from extractors.non_contextual_transformations import (
    trim_excessive_whitespace,
    download_and_save_cordis_pdfs,
)
from utils.config_loader import get_query_config
from utils.file_handling import unpack_and_remove_zip, load_file, write_file
from utils.logger import log_and_raise_exception
from utils.web_requests import (
    make_delete_request,
    make_get_request,
    download_file,
    get_base_url,
)


class CordisExtractor(IExtractor):
    """
    A Cordis extraction must follow this pattern:
    1. Call getExtraction
    2. Monitor getExtractionStatus until data is ready to be downloaded
    3. Download the data
    4. Store the checkpoint
    5. Call deleteExtraction
    """

    def __init__(self, extractor_name: str, checkpoint_name: str):
        super().__init__(extractor_name, checkpoint_name)

    def extract_until_next_checkpoint(self, query: str) -> None:
        api_key = os.getenv("API_KEY_CORDIS")
        if not api_key:
            return log_and_raise_exception("API Key not found")

        task_id = self._cordis_get_extraction_task_id(api_key, query)
        download_uri = self._cordis_get_download_uri(api_key, task_id)

        data_path = self.save_extracted_data(download_uri)
        self.non_contextual_transformation(data_path)

        checkpoint = self.get_new_checkpoint()
        self.save_checkpoint(checkpoint)

        self._cordis_delete_extraction(api_key, task_id)
        logging.info(">>> Successfully finished extraction")

    def restore_checkpoint(self) -> str:
        checkpoint = load_file(self.checkpoint_path + self.checkpoint_file)
        return checkpoint if checkpoint is not None else "1990-01-01"

    def create_next_checkpoint_end(self, next_checkpoint: str) -> str:
        last_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
        try:
            new_date = last_date.replace(year=last_date.year + int(next_checkpoint))
        except ValueError:
            new_date = last_date.replace(  # Handles February 29th for leap years
                year=last_date.year + int(next_checkpoint), day=28
            )
        return new_date.strftime("%Y-%m-%d")

    def save_extracted_data(self, data: str) -> str:
        base_url = "https://cordis.europa.eu/"
        zip_path = download_file(base_url + data, self.data_path)
        unpack_and_remove_zip(zip_path)

        # Cordis returns a zip in a fucking zip
        xml_zip_path = os.path.dirname(zip_path) + "/xml.zip"
        unpack_and_remove_zip(xml_zip_path)
        return self.data_path + "xml"

    def non_contextual_transformation(self, data_path: str):
        # Give each record its own directory
        for file_path in Path(data_path).iterdir():
            if not file_path.is_file() and not (file_path.suffix == ".xml"):
                log_and_raise_exception("We got a cordis record that is not an XML.")

            # New directory for record dataset
            record_dataset_dir = Path(self.data_path) / file_path.stem
            record_dataset_dir.mkdir(parents=True, exist_ok=True)

            # save transformed file in name dir with same name and delete original
            xml_content = load_file(str(file_path))
            xml_content_transformed = trim_excessive_whitespace(xml_content)
            write_file(
                str(record_dataset_dir / file_path.name), xml_content_transformed
            )

            # Get links
            link_dict_list = xml.extract_element_as_dict(str(file_path), "webLink")
            eu_links = [
                dic["physUrl"]["text"]
                for dic in link_dict_list
                if get_base_url(dic["physUrl"]["text"]) == "europa.eu"
            ]

            # if links, download all and save under /dir/attachment
            if eu_links:
                # New directory attachments
                attachment_dir = Path(self.data_path) / file_path.stem / "attachments"
                attachment_dir.mkdir(parents=True, exist_ok=True)

                for url in eu_links:
                    download_and_save_cordis_pdfs(
                        url,
                        str(attachment_dir),
                    )

            # remove file
            os.remove(file_path)

        # Remove xml directory
        shutil.rmtree(data_path)

    def get_new_checkpoint(self) -> str:
        date_elements = xml.get_all_elements_texts(self.data_path, self.checkpoint_name)
        date_objects = []
        for date_str in date_elements:
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    log_and_raise_exception(f"Could not parse date {date_str}")
                    date_obj = None

            date_objects.append(date_obj)

        if len(date_objects) != len(date_elements):
            log_and_raise_exception("Lost xml elements when converting to datatime.")

        if not date_objects:
            log_and_raise_exception("Lost xml elements when converting to datatime.")

        return max(date_objects).strftime("%Y-%m-%d")

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


def start_extraction(query: str, extractor_name: str, checkpoint_name: str):
    extractor = CordisExtractor(
        extractor_name=extractor_name, checkpoint_name=checkpoint_name
    )

    checkpoint_from = extractor.restore_checkpoint()
    checkpoint_to = extractor.create_next_checkpoint_end("5")

    base_query = f"{checkpoint_name}={checkpoint_from}-{checkpoint_to} AND "
    base_query += query

    extractor.extract_until_next_checkpoint(base_query)


def main():
    load_dotenv()
    config = get_query_config()["cordis"]

    query = config["queries"][0]
    extractor_name = f"cordis_{query.replace(' ', '')}"
    checkpoint_name = config["checkpoint"]

    for i in range(1):
        start_extraction(query, extractor_name, checkpoint_name)


if __name__ == "__main__":
    main()
