import argparse
import logging
import os
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

import lib.file_handling.file_parsing.xml_parser as xml
from interfaces.i_extractor import IExtractor
from lib.extractor.utils import trim_excessive_whitespace
from lib.file_handling.archive_utils import unpack_and_remove_zip
from lib.file_handling.file_utils import (
    load_file,
    write_file,
    ensure_path_exists,
    delete_if_empty,
)
from lib.requests.file_downloader import SeleniumFileDownloader
from lib.requests.requests import (
    make_delete_request,
    make_get_request,
    download_file,
    get_base_url,
)
from utils.config.config_loader import get_query_config
from utils.error_handling.error_handling import log_and_raise_exception


class CordisExtractor(IExtractor):
    """
    A Cordis extraction must follow this pattern:
    1. Call getExtraction
    2. Monitor getExtractionStatus until data is ready to be downloaded
    3. Download the data
    4. Call deleteExtraction
    """

    def __init__(
        self, extractor_name: str, checkpoint_name: str, download_attachments: bool
    ):
        super().__init__(extractor_name, checkpoint_name)
        self.download_attachments = download_attachments

    def extract_until_checkpoint_end(self, query: str) -> bool:
        api_key = os.getenv("API_KEY_CORDIS")
        if not api_key:
            log_and_raise_exception("API Key not found")

        task_id = self._cordis_get_extraction_task_id(api_key, query)
        download_uri, number_of_records = self._cordis_get_download_uri(
            api_key, task_id
        )

        data_path = self.save_extracted_data(download_uri)
        self.non_contextual_transformation(data_path)

        checkpoint = "2025-01-01"  # need to update and overwrite old projects
        self.save_checkpoint(checkpoint)

        self._cordis_delete_extraction(api_key, task_id)
        logging.info(">>> Successfully finished extraction")

        return number_of_records != 0

    def restore_checkpoint(self) -> str:
        checkpoint = load_file(self.checkpoint_path)
        return checkpoint if checkpoint is not None else "1990-01-01"

    def create_checkpoint_end_for_this_run(self, next_checkpoint: str) -> str:
        last_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
        try:
            new_date = last_date.replace(year=last_date.year + int(next_checkpoint))
        except ValueError:
            new_date = last_date.replace(  # Handles February 29th for leap years
                year=last_date.year + int(next_checkpoint), day=28
            )
        return new_date.strftime("%Y-%m-%d")

    def save_extracted_data(self, data: str) -> Path:
        # base_url = "https://cordis.europa.eu/"
        zip_path = download_file(data, self.data_path)
        unpack_and_remove_zip(zip_path)

        # Cordis returns a zip in a fucking zip
        xml_zip_path = zip_path.parent / "xml.zip"
        unpack_and_remove_zip(xml_zip_path)
        return self.data_path

    def non_contextual_transformation(self, data_path: Path):
        # xml_files_dir = data_path  # Store the directory containing the XML files
        for file_path in Path(data_path).iterdir():

            # ToDo:
            # need to overwrite the existing folders if we want to update in place

            if not file_path.is_file() or not (file_path.suffix == ".xml"):
                log_and_raise_exception("We got a cordis record that is not an XML")

            record_dataset_path = self.data_path / file_path.stem
            ensure_path_exists(record_dataset_path)

            xml_content_transformed = trim_excessive_whitespace(load_file(file_path))
            write_file(record_dataset_path / file_path.name, xml_content_transformed)

            self.download_and_save_attachments(file_path, record_dataset_path)
            os.remove(file_path)

        # if xml_files_dir != self.data_path:
        #     shutil.rmtree(xml_files_dir)

    def get_new_checkpoint_from_data(self) -> str:
        pass

    def parse_date_to_obj(self, date_str) -> datetime:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                log_and_raise_exception(f"Could not parse date {date_str}")

    def _cordis_get_extraction_task_id(self, key: str, query: str) -> str:
        """
        Calls Cordis getExtraction API and returns the task ID for the job and the number of records.
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

    def _cordis_get_download_uri(self, key: str, task_id: str) -> (str, int):
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
                return response["payload"]["destinationFileUri"], int(
                    response["payload"]["numberOfRecords"]
                )

            if response["payload"].get("error"):
                log_and_raise_exception(
                    f"Response error: {response['payload']['error']}"
                )

            if datetime.now() - start_time >= timedelta(hours=12):
                log_and_raise_exception(
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

    def download_and_save_attachments(self, file_path: Path, record_path: Path) -> None:
        if not self.download_attachments:
            return

        link_dicts = xml.extract_element_as_dict(file_path, "webLink")
        eu_links = [
            dic["webLink"]["physUrl"]
            for dic in link_dicts
            if get_base_url(dic["webLink"]["physUrl"]) == "europa.eu"
        ]
        if not eu_links:
            return

        attachment_dir = record_path / "attachments"
        if attachment_dir.exists():
            # logger.info(f"Attachment directory already exists for {record_path.stem}. Skipping download. ")
            return
        ensure_path_exists(attachment_dir)

        was_downloaded = []
        downloader = SeleniumFileDownloader(attachment_dir)
        for url in eu_links:
            was_downloaded.append(
                downloader.download_file(url, only_from_url="europa.eu")
            )
        downloader.close()
        delete_if_empty(attachment_dir)
        logging.info(
            f"Downloaded {len([d for d in was_downloaded if d])} files successfully and "
            f"{len([d for d in was_downloaded if not d])} files not successfully "
            f"for record dataset {record_path.stem}"
        )


def start_extraction(
    query: str,
    extractor_name: str,
    checkpoint_name: str,
    checkpoint_to_range: str,
    download_attachments: bool,
) -> bool:
    extractor = CordisExtractor(
        extractor_name=extractor_name,
        checkpoint_name=checkpoint_name,
        download_attachments=download_attachments,
    )

    checkpoint_from = extractor.restore_checkpoint()
    checkpoint_to = extractor.create_checkpoint_end_for_this_run(checkpoint_to_range)

    base_query = f"{checkpoint_name}={checkpoint_from}-{checkpoint_to} AND "
    base_query += query

    return extractor.extract_until_checkpoint_end(base_query)


def main(debug_2=0):
    parser = argparse.ArgumentParser(description="Run Cordis extract")
    parser.add_argument(
        "-r",
        "--run_id",
        type=int,
        default=0,
        help="Run ID to use from the config (default: 0)",
    )
    args = parser.parse_args()

    load_dotenv()

    config = get_query_config()["cordis"]
    run = config["runs"][args.run_id]
    query = run["query"]

    checkpoint_to_range = run["checkpoint_to_range"]
    download_attachments = run["download_attachments"]

    extractor_name = f"cordis_{query}"
    checkpoint_name = config["checkpoint"]

    continue_running = True
    while continue_running:
        continue_running = start_extraction(
            query,
            extractor_name,
            checkpoint_name,
            checkpoint_to_range,
            download_attachments,
        )
        debug_2 += 1
        if debug_2 == 2:
            break


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Critical error: {e}\n{traceback.format_exc()}")
