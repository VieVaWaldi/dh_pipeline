# import argparse
# import json
# import logging
# import os
# import time
# import traceback
# from datetime import datetime
# from pathlib import Path
# from typing import List, Dict, Any
#
# from dateutil.relativedelta import relativedelta
# from dotenv import load_dotenv
#
# from common_utils.config.config_loader import get_query_config
# from common_utils.error_handling.error_handling import log_and_raise_exception
# from core.file_handling import load_file
# from core.file_handling.file_parsing.json_parser import (
#     get_all_keys_value_recursively,
# )
# from core.web_requests import make_get_request
# from core.interfaces.extractor_interface import IExtractor
#
#
# class CoreExtractor(IExtractor):
#     """
#     A Core extraction must follow this pattern:
#     1. Call search which also returns the data (great)
#     However coreacuk has too many papers. One results returns more than the api allows,
#     so we limit it to 10k, remember the checkpoint and start the next query
#     sorted from there.
#     """
#
#     def __init__(
#         self, extractor_name: str, checkpoint_name: str, download_attachments: bool
#     ):
#         super().__init__(extractor_name, checkpoint_name)
#         self.api_key = os.getenv("API_KEY_CORE")
#         self.base_url = "https://api.core.ac.uk/v3"
#         self.headers = {"Authorization": f"Bearer {self.api_key}"}
#         self.download_attachments = download_attachments
#
#     def extract_until_next_checkpoint(self, query: str) -> bool:
#         if not self.api_key:
#             log_and_raise_exception("API Key not found")
#
#         core_data, total_hits = self._paginated_search_core(query)
#
#         if core_data:
#             data_path = self.save_extracted_data(core_data)
#             # self.non_contextual_transformation(data_path)
#
#             checkpoint = self.get_new_checkpoint_from_data()
#             self.save_checkpoint(checkpoint)
#
#             logging.info(">>> Successfully finished extraction")
#             return total_hits != 0
#         return False
#
#     def restore_checkpoint(self) -> str:
#         checkpoint = load_file(self.checkpoint_path)
#         return checkpoint if checkpoint is not None else "1990-01-01"
#
#     def create_checkpoint_end_for_this_run(self, next_checkpoint: str) -> str:
#         days_to_add = int(next_checkpoint)
#         last_checkpoint_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
#         new_checkpoint_date = last_checkpoint_date + relativedelta(days=days_to_add)
#         return new_checkpoint_date.strftime("%Y-%m-%d")
#
#     def save_extracted_data(self, data: List[Dict[str, Any]]) -> Path:
#         for index, entry in enumerate(data):
#             title = self.clean_title(entry["title"], entry, index)
#             date = (
#                 self.parse_date_to_obj(entry["publishedDate"]).strftime("%Y-%m-%d")
#                 if entry.get("publishedDate")
#                 else f"NO-DATE"
#             )
#             filename = f"{date}_{title}.json"
#             file_path = os.path.join(self.data_path, filename)
#             with open(file_path, "w", encoding="utf-8") as f:
#                 json_string = json.dumps(entry, indent=4, ensure_ascii=False)
#                 f.write(json_string)
#
#         logging.info(
#             f"Saved {len(data)} entries as individual files in {self.data_path}",
#         )
#         return Path("")
#
#     def non_contextual_transformation(self, data_path: str):
#         pass
#
#     def get_new_checkpoint_from_data(self) -> str:
#         date_elements = get_all_keys_value_recursively(
#             self.data_path, self.checkpoint_name
#         )
#         date_objects = []
#         for date_str in date_elements:
#             date_objects.append(self.parse_date_to_obj(date_str[0]))
#
#         if not date_objects:
#             log_and_raise_exception("Lost json elements when converting to datatime.")
#
#         return max(date_objects).strftime("%Y-%m-%d")
#
#     def _paginated_search_core(self, query: str) -> (List[Dict[str, Any]], int):
#         all_data = []
#         total_hits = 0
#         offset = 0
#
#         while True:
#             core_data, hits = self._search_core(query, offset)
#             if not core_data:
#                 break
#
#             all_data.extend(core_data)
#             total_hits = hits
#             offset += len(core_data)
#
#             if offset >= total_hits or offset >= 2000:  # Respect the 2000 item limit
#                 break
#
#         return all_data, total_hits
#
#     def _search_core(
#         self, query: str, offset: int = 0, chunk_size: int = 100
#     ) -> (List[Dict[str, Any]], int):
#         params = {
#             "q": query,
#             "sort": "publishedDate",
#             "limit": chunk_size,
#             "offset": offset,
#         }
#         response = make_get_request(
#             f"{self.base_url}/search/works", params, self.headers
#         )
#
#         return response["results"], response["totalHits"]
#
#     def clean_title(self, param, entry, index):
#         return (
#             param.replace(" ", "").replace("/", "").replace(".", "").lower()[:30]
#             if entry.get("title")
#             else f"NO-TITLE_{index}"
#         )
#
#     def parse_date_to_obj(self, date_string: str) -> datetime:
#         try:
#             return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
#         except ValueError:
#             try:
#                 return datetime.strptime(date_string, "%Y-%m-%d")
#             except ValueError:
#                 raise ValueError(f"Unable to parse date string: {date_string}")
#
#
# def start_extraction(
#     query: str,
#     extractor_name: str,
#     checkpoint_name: str,
#     checkpoint_to_range: str,
#     download_attachments: bool,
# ) -> bool:
#     extractor = CoreExtractor(
#         extractor_name=extractor_name,
#         checkpoint_name=checkpoint_name,
#         download_attachments=download_attachments,
#     )
#
#     checkpoint_from = extractor.restore_checkpoint()
#     checkpoint_to = extractor.create_checkpoint_end_for_this_run(checkpoint_to_range)
#
#     base_query = f"({checkpoint_name}>{checkpoint_from} AND {checkpoint_name}<{checkpoint_to}) AND "
#     base_query += query
#
#     return extractor.extract_until_next_checkpoint(base_query)
#
#
# def main():
#     parser = argparse.ArgumentParser(description="Run Core extractor")
#     parser.add_argument(
#         "-r",
#         "--run_id",
#         type=int,
#         default=0,
#         help="Run ID to use from the config (default: 0)",
#     )
#     args = parser.parse_args()
#
#     load_dotenv()
#     config = get_query_config()["coreacuk"]
#
#     run = config["runs"][args.run_id]
#     query = run["query"]
#
#     checkpoint_to_range = run["checkpoint_to_range"]
#     download_attachments = run["download_attachments"]
#
#     extractor_name = f"core_{query}"
#     checkpoint_name = config["checkpoint"]
#
#     continue_running = True
#     while continue_running:
#         continue_running = start_extraction(
#             query,
#             extractor_name,
#             checkpoint_name,
#             checkpoint_to_range,
#             download_attachments,
#         )
#
#         # To respect the coreacuk limit
#         time.sleep(60)
#
#
# if __name__ == "__main__":
#     try:
#         main()
#     except Exception as e:
#         logging.critical(f"Critical error: {e}\n{traceback.format_exc()}")
