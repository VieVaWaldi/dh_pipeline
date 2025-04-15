import argparse
import csv
from collections import defaultdict
from typing import Dict, Any

from analysis.utils.analysis_interface import IAnalysisJob
from analysis.utils.analysis_utils import clean_value
from core.file_handling.general_parser import yield_all_documents


class KeysFrequencyAnalysis(IAnalysisJob):
    """
    A job to analyze a dictionaries keys, statistics about each unique keys usage
    and respective values content including samples. A sample is gathered
    for each first value in a checkpoint that is not None or empty.
    The results are saved to a CSV file.
    """

    def __init__(
        self,
        query_name: str,
        analysis_name: str,
        cordis_only_project_flag: bool = False,
    ):
        super().__init__(analysis_name, query_name)
        self.keys_statistics: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "occurrences": 0,
                "is_list": " ",
                "total_length": 0,
                "max_length": 0,
                "samples": [],
            }
        )
        self.total_files = 0
        self.num_samples = 10
        self.cordis_only_project_flag = cordis_only_project_flag

    def run(self):
        for idx, (document, path) in enumerate(
            yield_all_documents(self.data_path, self.cordis_only_project_flag)
        ):
            self.traverse_dictionary(document)
            self.total_files += 1
            if idx % 20_000 == 0:
                print(f"Processed {idx} files")

        self.update_avg_length_per_key()
        self.save_output()

    def traverse_dictionary(self, document: Dict[str, Any], parent_key: str = ""):
        for key, value in document.items():
            current_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, dict):
                self.traverse_dictionary(value, current_key)

            elif isinstance(value, list):
                self.update_list_stats(current_key, -1)
                list_items = 0
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self.traverse_dictionary(item, f"{current_key}[_]")
                    else:
                        self.update_tag_stats(f"{current_key}[_]", str(item))
                    list_items += 1
                self.update_list_stats(current_key, list_items)
            else:
                self.update_tag_stats(current_key, str(value))

    def update_tag_stats(self, key: str, content: str):
        stats = self.keys_statistics[key]
        stats["occurrences"] += 1
        content_length = len(content)
        stats["total_length"] += content_length
        stats["max_length"] = max(stats["max_length"], content_length)
        if len(stats["samples"]) < self.num_samples and content and content != "":
            stats["samples"].append(clean_value(content[:30]))

    def update_list_stats(self, list_key: str, list_items: int):
        stats = self.keys_statistics[list_key]
        if list_items == -1:
            return  # only save tag to have it at the right position
        stats["occurrences"] += 1
        stats["is_list"] = True
        stats["total_length"] += list_items
        stats["max_length"] = max(stats["max_length"], list_items)

    def update_avg_length_per_key(self):
        for key in self.keys_statistics.values():
            key["avg_length"] = (
                key["total_length"] / key["occurrences"]
                if key["occurrences"] > 0
                else 0
            )

    def save_output(self):
        output_file = self.output_path / f"{self.analysis_name}_results.csv"
        unique_keys_count = len(self.keys_statistics)
        rows_written = 0

        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            header = [
                "Tag",
                "Occurrences",
                "Is List?",
                "Average Length",
                "Max Length",
                "Samples",
            ]
            writer.writerow(header)

            for tag, stats in self.keys_statistics.items():
                row = [
                    tag,
                    stats["occurrences"],
                    stats["is_list"],
                    f"{stats['avg_length']:.2f}" if stats["occurrences"] > 0 else "0",
                    stats["max_length"],
                ]
                row.extend(stats["samples"])
                writer.writerow(row)
                rows_written += 1

        if rows_written != unique_keys_count:
            msg = f"Warning: Mismatch in number of keys ({unique_keys_count}) and rows written ({rows_written})"
        else:
            msg = f"Verification successful: {rows_written} rows written, matching {unique_keys_count} unique keys"
        print(f"{msg}\nProcessed {self.total_files} documents.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Keys Analysis")
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="Choose query to run analysis on",
        # cordis_culturalORheritage
        # arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB
        # core_LBLBcomputingANDculturalRBORLBcomputingANDheritageRBRB
        default="core_LBLBcomputingANDculturalRBORLBcomputingANDheritageRBRB",
    )
    args = parser.parse_args()

    job = KeysFrequencyAnalysis(
        args.query, "keys_frequency_analysis", cordis_only_project_flag=True
    )
    job.run()
