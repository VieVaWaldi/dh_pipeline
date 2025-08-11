import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, List

from interfaces.i_extractor import IExtractor, ExtractorConfig
from lib.file_handling.file_utils import (
    ensure_path_exists,
    write_file,
)
from lib.requests.requests import make_get_request
from lib.sanitizers.parse_file_names import parse_file_names


class OpenAIREExtractor(IExtractor):
    """
    OpenAIRE Extractor - Extract project data and research products from OpenAIRE API.

    Uses year-based checkpointing to handle API limitations:
    - Extracts all projects from a specific year
    - Handles pagination within each year (up to 10,000 results)
    - Progresses year by year according to checkpoint_range
    - Resets to ~5 years ago when no more future data is found

    For each project:
    1. Extract project metadata
    2. Fetch related research products
    3. Save both in structured folders
    """

    def __init__(self, extractor_config: ExtractorConfig):
        super().__init__(extractor_config, sleep_between_extractions=5)
        self.base_project_url = "https://api.openaire.eu/search/projects"
        self.base_research_products_url = (
            "https://api.openaire.eu/graph/v1/researchProducts"
        )
        self.page_size = 10
        self.max_results_per_year = 10000

    def should_continue(self) -> bool:
        """Continue while next checkpoint < today + 10 years"""
        ten_years_future = datetime.now().year + 10
        next_year = self.get_checkpoint_end()
        return next_year <= ten_years_future

    def extract_until_next_checkpoint(self) -> bool:
        current_year = int(self.checkpoint)
        logging.info(
            f"Starting extraction for year {current_year} "
            f"(range: {self.checkpoint_range} years)"
        )

        self._fetch_projects(current_year)
        should_continue = self.should_continue()

        if should_continue:
            # Normal progression: move to next year
            new_checkpoint = str(self.get_checkpoint_end())
            self.save_checkpoint(new_checkpoint)
            logging.info(f"Advanced checkpoint to year: {new_checkpoint}")
        else:
            # No more data in future, reset to collect updates
            reset_checkpoint = self._get_reset_checkpoint()
            self.save_checkpoint(reset_checkpoint)
            logging.info(
                f"No more future data. Reset checkpoint to year {reset_checkpoint} "
                f"for update collection"
            )

        return should_continue

    def get_checkpoint_end(self, minus_1_day: bool = False) -> int:
        """
        Calculate the end year for current checkpoint range.
        Note: minus_1_day parameter kept for interface compatibility but not used for years.
        """
        current_year = int(self.checkpoint)
        return current_year + int(self.checkpoint_range)

    def string_to_checkpoint(self, checkpoint_str: str) -> int:
        """Convert checkpoint string to year integer."""
        return int(checkpoint_str)

    def checkpoint_to_string(self, checkpoint_year: int) -> str:
        """Convert year integer to checkpoint string."""
        return str(checkpoint_year)

    def _get_reset_checkpoint(self) -> str:
        """
        Find a checkpoint year from our progression that's approximately 5 years ago.
        This ensures we reuse existing checkpoint directories for overwriting.
        """
        target_year = datetime.now().year - 5

        # Reconstruct our checkpoint progression from start
        current = self.string_to_checkpoint(self.checkpoint_start)
        best_checkpoint = self.checkpoint_start
        min_diff = abs(current - target_year)

        # Walk through the progression to find closest to 5 years ago
        while current < datetime.now().year:
            diff = abs(current - target_year)
            if diff < min_diff:
                min_diff = diff
                best_checkpoint = self.checkpoint_to_string(current)
            current += int(self.checkpoint_range)

        logging.info(
            f"Found reset checkpoint {best_checkpoint} (~5 years ago from today)"
        )
        return best_checkpoint

    def _fetch_projects(self, current_year: int):
        total_projects_found = 0
        page = 1
        while True:
            try:
                projects, total_in_year, current_page = self._fetch_projects_for_year(
                    current_year, page
                )

                if not projects:
                    logging.info(
                        f"No more projects on page {page} for year {current_year}"
                    )
                    break

                for project in projects:
                    self._process_project(project)
                    total_projects_found += 1

                if page * self.page_size >= total_in_year:
                    logging.info(
                        f"Retrieved all {total_in_year} projects for year {current_year}"
                    )
                    break
                if page * self.page_size >= self.max_results_per_year:
                    logging.warning(
                        f"Reached API limit of {self.max_results_per_year} results for year {current_year}"
                    )
                    break

                page += 1
                time.sleep(1)  # Rate limiting between pages

            except Exception as e:
                logging.error(
                    f"Error processing page {page} of year {current_year}: {e}"
                )
                break
        logging.info(
            f"Processed {total_projects_found} projects for year {current_year}"
        )

    def _fetch_projects_for_year(self, year: int, page: int) -> Tuple[List, int, int]:
        """
        Fetch projects that started in a specific year using pagination.
        Returns (projects_list, total_projects, current_page).
        """
        params = {
            "keywords": self.query,
            "startYear": year,
            "sortBy": "projectstartdate,ascending",
            "format": "json",
            "page": page,
            "size": self.page_size,
        }

        logging.debug(f"Fetching projects: year={year}, page={page}")

        try:
            response = make_get_request(self.base_project_url, params, timeout=300)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error on page {page} of year {year}, skipping page: {e}")
            return [], 0, page
        except Exception as e:
            logging.error(f"Request error on page {page} of year {year}, skipping page: {e}")
            return [], 0, page

        try:
            header = response.get("response", {}).get("header", {})
            total_projects = int(header.get("total", {}).get("$", 0))
            current_page = int(header.get("page", {}).get("$", page))

            results = response.get("response", {}).get("results", {})

            if "result" in results:
                result = results["result"]
                if isinstance(result, dict):
                    projects = [result]
                elif isinstance(result, list):
                    projects = result
                else:
                    projects = []
            else:
                projects = []

            logging.debug(
                f"Retrieved {len(projects)} projects "
                f"(page {current_page}, total: {total_projects})"
            )
            return projects, total_projects, current_page

        except (KeyError, TypeError, ValueError) as e:
            logging.error(f"Error parsing projects response: {e}")
            return [], 0, page

    def _process_project(self, project: Dict[str, Any]):
        """
        Process a single project: extract info, create folder, save data.
        """
        try:
            project_info = self._extract_project_info(project)
            if not project_info["id"]:
                logging.warning("Could not extract project ID, skipping project")
                return

            project_folder = self._create_project_folder(project_info)

            project_json = self._normalize_json(project)
            project_file = project_folder / "project.json"
            write_file(project_file, project_json)
            logging.info(f"Saved project: {project_folder.name}")

            research_products = self._fetch_research_products(project_info["id"])
            if research_products:
                research_json = self._normalize_json(research_products)
                research_file = project_folder / "research_products.json"
                write_file(research_file, research_json)
                logging.info(f"Saved {len(research_products)} research products")

        except Exception as e:
            logging.error(f"Error processing project: {e}")

    def _extract_project_info(self, project: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract key information from project data for folder naming and processing.
        Returns dict with 'id', 'start_date', and 'title'.
        """
        # Extract project ID
        project_id = None
        try:
            if "header" in project and "dri:objIdentifier" in project["header"]:
                project_id = project["header"]["dri:objIdentifier"]["$"]
        except (KeyError, TypeError):
            pass

        if not project_id and "id" in project:
            project_id = project.get("id")

        # Navigate to the project data
        project_data = {}
        try:
            project_data = (
                project.get("metadata", {}).get("oaf:entity", {}).get("oaf:project", {})
            )
        except (AttributeError, TypeError):
            pass

        # Extract title
        title = "unknown_project"
        try:
            if "title" in project_data and "$" in project_data["title"]:
                title = project_data["title"]["$"]
        except (KeyError, TypeError):
            pass

        # Extract start date
        start_date = "1900-01-01"
        try:
            if "startdate" in project_data and "$" in project_data["startdate"]:
                date_str = project_data["startdate"]["$"]
                if len(date_str) >= 10:
                    try:
                        date_obj = datetime.strptime(date_str[:10], "%Y-%m-%d")
                        start_date = date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
        except (KeyError, TypeError):
            pass

        return {"id": project_id, "start_date": start_date, "title": title}

    def _create_project_folder(self, project_info: Dict[str, str]) -> Path:
        """
        Create folder for project following naming convention:
        YYYY-MM-DD-sanitize(title)
        """
        date_str = project_info["start_date"]
        title = parse_file_names(project_info["title"])

        folder_name = f"{date_str}-{title[:80]}"
        project_path = self.data_path / folder_name
        ensure_path_exists(project_path)

        return project_path

    def _fetch_research_products(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Fetch all research products related to a specific project.
        Uses cursor-based pagination to handle large result sets.
        """
        all_results = []
        cursor = "*"

        while cursor:
            params = {
                "relProjectId": project_id,
                "pageSize": self.page_size,
                "cursor": cursor,
            }

            try:
                response = make_get_request(
                    self.base_research_products_url,
                    params,
                    can_fail=True,
                )

                if not response:
                    break

                if "results" in response and response["results"]:
                    all_results.extend(response["results"])

                header = response.get("header", {})
                cursor = header.get("nextCursor")

                if not cursor:
                    break

                time.sleep(0.5)

            except Exception as e:
                logging.warning(
                    f"Error fetching research products for project {project_id}: {e}"
                )
                break

        if all_results:
            logging.debug(f"Retrieved {len(all_results)} research products for project")

        return all_results

    def _normalize_json(self, data: Any) -> str:
        """
        Normalize JSON data to handle Unicode properly.
        Ensures characters are displayed correctly instead of escape sequences.
        """
        return json.dumps(data, indent=2, ensure_ascii=False)
