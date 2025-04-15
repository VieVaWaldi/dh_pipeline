import argparse
import logging
import shutil
import time
import xml.etree.ElementTree as xml
from abc import ABC
from pathlib import Path
from typing import Any, List, Dict

import requests
from dotenv import load_dotenv

from common_utils.config.config_loader import get_query_config
from common_utils.error_handling.error_handling import log_and_raise_exception
from core.file_handling.file_handling import load_file, write_file
from core.sanitizers.sanitizer import (
    trim_excessive_whitespace,
)
from interfaces import IExtractor


class ArxivExtractor(IExtractor, ABC):
    def __init__(
        self, extractor_name: str, checkpoint_name: str, download_attachments: bool
    ):
        super().__init__(extractor_name, checkpoint_name)
        self.processed_ids_file = self.data_path / "processed_ids.txt"
        self.download_attachments = download_attachments

    def extract_until_next_checkpoint(self, query: str) -> bool:
        xml_content = self.fetch_arxiv_data(query)

        meta_data = self.print_arxiv_meta_data(xml_content)
        total_results = meta_data["total_results"]

        entries = self.extract_entries(xml_content)
        self.check_and_save_new_entries(entries)

        if not self.get_new_checkpoint_from_data() < total_results:
            logging.info(">>> Finished extraction, no more data")
            return False

        self.save_checkpoint(str(self.get_new_checkpoint_from_data()))

        # Rate Limiting: Fetch at once a batch of 2000 entries and add 3 seconds delay before fetching a new batch.
        # Log a completion message once all pages/entries have been processed.
        time.sleep(30)
        logging.info(">>> Successfully finished extraction")
        return True

    def restore_checkpoint(self) -> str:
        checkpoint = load_file(self.checkpoint_path)
        return checkpoint if checkpoint is not None else "0"

    def get_new_checkpoint_from_data(self) -> int:
        return int(self.last_checkpoint) + 2000

    def create_checkpoint_end_for_this_run(self, next_checkpoint: str) -> str:
        """
        Not needed when working with offsets.
        """
        raise NotImplementedError()

    def check_and_save_new_entries(self, entries):
        """
        Check for new entries (not processed) with the help of IDS
        Save new entries
        Update the set of processed IDs
        """
        new_entries = []
        processed_ids = self.get_processed_ids()
        for entry in entries:
            entry_element = xml.fromstring(entry)
            entry_id = entry_element.find("{http://www.w3.org/2005/Atom}id").text
            if entry_id not in processed_ids:
                new_entries.append(entry)
                processed_ids.add(entry_id)

        if new_entries:
            self.save_entries_and_papers(new_entries)
            self.save_processed_ids(processed_ids)
        else:
            logging.info("No new entries found.")

    def fetch_arxiv_data(self, query: str) -> str:
        try:
            logging.info(
                f"Fetching results {self.last_checkpoint} to {self.get_new_checkpoint_from_data()}..."
            )
            url = f"https://export.arxiv.org/api/query?{query}"
            response = requests.get(url)
            response.raise_for_status()
            logging.info(
                "GET Request status: %s",
                response.text.replace("\n", " ")[:512],
            )
            return response.text
        except requests.RequestException as e:
            log_and_raise_exception(f"Error fetching data from {url}: {e}")

    # Extract metadata as a dictionary: total_results, start_index, items_per_page
    # Log any errors
    def print_arxiv_meta_data(self, xml_content: str) -> Dict[str, Any]:
        try:
            root = xml.fromstring(xml_content)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
            }
            total_results = int(root.find("opensearch:totalResults", ns).text)
            start_index = int(root.find("opensearch:startIndex", ns).text)
            items_per_page = int(root.find("opensearch:itemsPerPage", ns).text)
            logging.info(f"Total Results: {total_results}")
            logging.info(f"Start Index: {start_index}")
            logging.info(f"Items Per Page: {items_per_page}")
            return {
                "total_results": total_results,
                "start_index": start_index,
                "items_per_page": items_per_page,
            }
        except Exception as e:
            log_and_raise_exception(f"Error parsing metadata: {e}")

    # Parse XML content, extract individual entries and return them as a list of strings
    # Log any errors
    def extract_entries(self, xml_content: str) -> List[str]:
        try:
            root = xml.fromstring(xml_content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            return [xml.tostring(entry, encoding="unicode") for entry in entries]
        except Exception as e:
            log_and_raise_exception(f"Error extracting entries: {e}")

    def save_entries_and_papers(self, entries: List[str]) -> None:
        for entry in entries:
            # Parse the XML entry
            try:
                entry_element = xml.fromstring(
                    entry
                )  # Convert XML string into ElementTree object
                title = entry_element.find(
                    "{http://www.w3.org/2005/Atom}title"
                ).text  # Extract title
                category = entry_element.find(
                    "{http://www.w3.org/2005/Atom}category"
                ).attrib.get(
                    "term", "No Category"
                )  # Extract Category. If no category found: No category
                published_date = entry_element.find(
                    "{http://www.w3.org/2005/Atom}published"
                ).text  # Extract published date
                safe_title = (
                    f"{published_date}_"
                    + "".join([c if c.isalnum() else "_" for c in title])[:40]
                )  # title: replace non-alphanumeric characters with underscores

                # Create a directory for an entry
                entry_dir = self.data_path / safe_title
                entry_dir.mkdir(
                    parents=True, exist_ok=True
                )  # Creates the directory if it doesn’t already exist.

                # Define the path for the XML file of the entry
                xml_file_path = entry_dir / f"entry_{safe_title}.xml"

                # Add the 'category_term' element to the XML file
                category_element = xml.Element(
                    "category_term"
                )  # Creates a new XML element for the category term.
                category_element.text = (
                    category  # Sets the text of the new category element
                )
                entry_element.append(
                    category_element
                )  # Appends the new category element to the original XML entry.
                tree = xml.ElementTree(
                    entry_element
                )  # Creates an ElementTree object from the updated XML element.
                tree.write(
                    xml_file_path, encoding="utf-8", xml_declaration=True
                )  # Writes the XML data to the specified file path

                # Find and save the PDF file
                pdf_url = None  # Initializes the PDF URL variable
                # Iterates over all link elements in the XML
                for link in entry_element.findall("{http://www.w3.org/2005/Atom}link"):
                    if (
                        link.attrib.get("title") == "pdf"
                    ):  # Checks if the link’s title attribute is 'pdf'.
                        pdf_url = link.attrib["href"]  # Extracts the URL if found.
                        break

                # IF found, download and save the PDF File
                if pdf_url:
                    try:
                        pdf_file_path = (
                            entry_dir / f"paper_{safe_title}.pdf"
                        )  # Defines the path for the PDF file
                        response = requests.get(
                            pdf_url
                        )  # GET request to download the PDF.
                        response.raise_for_status()
                        with pdf_file_path.open("wb") as f:
                            f.write(response.content)
                        logging.info(f"Saved {pdf_file_path}")
                    except Exception as e:
                        logging.info("Couldnt download pdf.")

            except Exception as e:
                log_and_raise_exception(f"Error saving entries and papers to file: {e}")

    # Manage a record of IDs to prevent duplication
    # Update the file with new processed IDs
    def get_processed_ids(self) -> set:
        if self.processed_ids_file.exists():
            return {line.strip() for line in self.processed_ids_file.open("r")}
        return set()

    def save_processed_ids(self, ids: set) -> None:
        with self.processed_ids_file.open("a") as f:
            for id in ids:
                f.write(f"{id}\n")

    def save_extracted_data(self, data: str | Dict[str, Any]) -> Path:
        raise NotImplementedError

    # Trim excessive whitespace from the XML content
    def non_contextual_transformation(self, data_path: Path):
        # iteriate over each item in directoy data_path
        for entry_dir in Path(data_path).iterdir():
            if not entry_dir.is_dir():
                log_and_raise_exception(f"Expected directory but found {entry_dir}")

        # within each directory iterates over files to find xml suffix
        for file_path in entry_dir.iterdir():
            if not file_path.is_file() or file_path.suffix != ".xml":
                log_and_raise_exception(f"Found non-XML file: {file_path}")

            # Read and trim excessive whitespace from XML content
            xml_content_transformed = trim_excessive_whitespace(load_file(file_path))

            # Write the transformed XML content back to the file
            write_file(file_path, xml_content_transformed)

        # Remove the original directory after processing all files
        shutil.rmtree(entry_dir)


def start_extraction(
    query: str,
    extractor_name: str,
    checkpoint_name: str,
    checkpoint_to_range: str,
    download_attachments: bool,
) -> bool:
    extractor = ArxivExtractor(
        extractor_name=extractor_name,
        checkpoint_name=checkpoint_name,
        download_attachments=download_attachments,
    )

    checkpoint_from = extractor.restore_checkpoint()
    # checkpoint_to = extractor.create_checkpoint_end_for_this_run(checkpoint_to_range)

    base_query = f"search_query={query}"
    base_query += f"&start={checkpoint_from}&max_results={checkpoint_to_range}"
    base_query += f"&sortBy=submittedDate&sortOrder=ascending"
    base_query += f"&start_date=1990-01-01"

    return extractor.extract_until_next_checkpoint(base_query)


def main():
    parser = argparse.ArgumentParser(description="Run Arxiv extractor")
    parser.add_argument(
        "-r",
        "--run_id",
        type=int,
        default=0,
        help="Run ID to use from the config (default: 0)",
    )
    args = parser.parse_args()

    load_dotenv()
    config = get_query_config()["arxiv"]

    run = config["runs"][args.run_id]

    query = run["query"]
    checkpoint_to_range = run["checkpoint_to_range"]
    download_attachments = run["download_attachments"]

    extractor_name = f"arxiv_{query}"
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


if __name__ == "__main__":
    main()
