import argparse
import json
import logging
import time
import traceback
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

from dotenv import load_dotenv

from core.file_handling.file_handling import (
    ensure_path_exists,
    load_file,
    write_file,
)
from core.web_requests.web_requests import (
    make_get_request,
)
from interfaces.i_extractor import IExtractor
from utils.config.config_loader import get_query_config
from utils.error_handling.error_handling import log_and_raise_exception


class OpenAIREExtractor(IExtractor):
    """
    OpenAIRE Extractor - Extract project data and related research products from OpenAIRE API

    This extractor follows this pattern:
    1. Extract projects based on query keywords
    2. For each project, extract its related research products
    3. Save both project and research product data
    4. Handle pagination to extract all results
    5. Handle rate limiting by reporting current progress
    """

    def __init__(
        self, extractor_name: str, checkpoint_name: str, download_attachments: bool
    ):
        super().__init__(extractor_name, checkpoint_name)
        # self.download_attachments = download_attachments
        self.base_project_url = "https://api.openaire.eu/search/projects"
        self.base_research_products_url = (
            "https://api.openaire.eu/graph/researchProducts"
        )
        self.page_size = 50  # Default page size for project queries

    def extract_until_next_checkpoint(self, query: str) -> bool:
        """
        Extract projects and their research products until we reach the next checkpoint.
        Returns True if there are more results to fetch, False otherwise.
        """
        # Parse checkpoint to determine current page
        current_page = (
            int(self.last_checkpoint) if self.last_checkpoint.isdigit() else 1
        )

        try:
            # Get projects from OpenAIRE API
            projects, total_projects, page = self._fetch_projects(query, current_page)

            if not projects:
                logging.info("No projects found or reached the end of results")
                return False

            # Process each project
            for project in projects:
                project_id = self._extract_project_id(project)
                if not project_id:
                    logging.warning("Could not extract project ID, skipping project")
                    continue

                # Create project directory
                project_dir = self.data_path / project_id
                ensure_path_exists(project_dir)

                # Save project data
                project_json = self._normalize_json(project)
                write_file(project_dir / "project.json", project_json)

                # Get research products for this project
                research_products = self._fetch_research_products(project_id)

                # Save research products data if any exist
                if research_products:
                    research_products_json = self._normalize_json(research_products)
                    write_file(
                        project_dir / "research_products.json", research_products_json
                    )

            # Save the next page as checkpoint
            next_page = page + 1
            if next_page * self.page_size <= total_projects:
                self.save_checkpoint(str(next_page))
                logging.info(f"Saved checkpoint for page {next_page}")
                return True
            else:
                logging.info("No more pages to process")
                return False

        except Exception as e:
            # On error, print the current page, size, and total
            logging.error(f"Error during extraction: {e}")
            logging.error(f"Current page: {current_page}, size: {self.page_size}")
            logging.error(traceback.format_exc())

            # Save current page as checkpoint to resume later
            self.save_checkpoint(str(current_page))
            return False

    def _fetch_projects(self, query: str, page: int) -> Tuple[list, int, int]:
        """
        Fetch projects from OpenAIRE API using pagination.
        Returns a tuple of (projects_list, total_projects, current_page)
        """
        params = {
            "keywords": query,
            "sortBy": "projectstartyear,ascending",
            "format": "json",
            "page": page,
            "size": self.page_size,
            # "startYear": 2020
        }

        response = make_get_request(self.base_project_url, params)

        try:
            total_projects = int(response["response"]["header"]["total"]["$"])
            current_page = int(response["response"]["header"]["page"]["$"])

            # Extract projects list
            if "result" in response["response"]["results"]:
                return (
                    response["response"]["results"]["result"],
                    total_projects,
                    current_page,
                )
            else:
                return [], total_projects, current_page
        except (KeyError, TypeError) as e:
            log_and_raise_exception(f"Error parsing projects response: {e}")

    def _fetch_research_products(self, project_id: str) -> list:
        """
        Fetch research products related to a specific project.
        Uses cursor-based pagination to handle large result sets.
        """
        all_results = []
        page_size = 100  # Use larger page size for research products
        cursor = None

        while True:
            params = {"relProjectId": project_id, "pageSize": page_size}

            if cursor:
                params["cursor"] = cursor

            try:
                response = make_get_request(self.base_research_products_url, params)

                # Extract results from response
                if "results" in response and response["results"]:
                    all_results.extend(response["results"])

                # Check if there's a next cursor for pagination
                if "header" in response and "nextCursor" in response["header"]:
                    cursor = response["header"]["nextCursor"]
                    # Add a small delay to avoid hitting rate limits
                    time.sleep(1)
                else:
                    break
            except Exception as e:
                logging.warning(
                    f"Error fetching research products for project {project_id}: {e}"
                )
                break

        return all_results

    def _extract_project_id(self, project: Dict) -> Optional[str]:
        """
        Extract the project ID from the project data.
        Returns None if ID cannot be found.
        """
        try:
            if "header" in project and "dri:objIdentifier" in project["header"]:
                return project["header"]["dri:objIdentifier"]["$"]
        except (KeyError, TypeError):
            pass

        # Fallback methods if the expected path doesn't exist
        try:
            if "id" in project:
                return project["id"]
        except (KeyError, TypeError):
            pass

        return None

    def _normalize_json(self, data: Any) -> str:
        """
        Normalize JSON data to handle Unicode escape sequences properly.
        This ensures that characters like 'ção' are properly displayed instead of showing as \u00e7\u00e3o.
        """
        # Use ensure_ascii=False to keep Unicode characters as-is
        # Use a high indent value for better readability
        return json.dumps(data, indent=2, ensure_ascii=False)

    def restore_checkpoint(self) -> str:
        """
        Load the checkpoint (page number) from the checkpoint file.
        Return "1" if no checkpoint exists.
        """
        checkpoint = load_file(self.checkpoint_path)
        return checkpoint if checkpoint is not None else "1"

    def create_checkpoint_end_for_this_run(self, next_checkpoint: str) -> str:
        """
        For OpenAIRE, we use pagination with page numbers, so this method
        calculates the target page number for this run.
        """
        current_page = (
            int(self.last_checkpoint) if self.last_checkpoint.isdigit() else 1
        )
        target_page = current_page + int(next_checkpoint)
        return str(target_page)

    def save_extracted_data(self, data: Dict[str, Any]) -> Path:
        """
        Not directly used in our implementation, but required by the interface.
        We save data directly in extract_until_next_checkpoint.
        """
        return self.data_path

    def non_contextual_transformation(self, data_path: Path) -> None:
        """
        No transformation needed for OpenAIRE data.
        """
        pass

    def get_new_checkpoint_from_data(self) -> str:
        """
        Not directly used in our implementation, but required by the interface.
        We manage checkpoints directly in extract_until_next_checkpoint.
        """
        return self.last_checkpoint


def start_extraction(
    query: str,
    extractor_name: str,
    checkpoint_name: str,
    checkpoint_to_range: str,
    download_attachments: bool,
) -> bool:
    extractor = OpenAIREExtractor(
        extractor_name=extractor_name,
        checkpoint_name=checkpoint_name,
        download_attachments=download_attachments,
    )

    # checkpoint_from = extractor.restore_checkpoint()
    # checkpoint_to = extractor.create_checkpoint_end_for_this_run(checkpoint_to_range)

    # For OpenAIRE, we use the raw query string
    return extractor.extract_until_next_checkpoint(query)


def main():
    parser = argparse.ArgumentParser(description="Run OpenAIRE extractor")
    parser.add_argument(
        "-r",
        "--run_id",
        type=int,
        default=0,
        help="Run ID to use from the config (default: 0)",
    )
    args = parser.parse_args()

    load_dotenv()

    config = get_query_config()["openaire"]
    run = config["runs"][args.run_id]
    query = run["query"]

    checkpoint_to_range = run["checkpoint_to_range"]
    download_attachments = run["download_attachments"]

    extractor_name = f"openaire_{query}"
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
    try:
        main()
    except Exception as e:
        logging.critical(f"Critical error: {e}\n{traceback.format_exc()}")
