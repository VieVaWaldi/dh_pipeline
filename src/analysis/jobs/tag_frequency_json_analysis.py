import argparse
import csv
from collections import defaultdict
from typing import Dict, Any

from analysis.jobs.analysis_interface import IAnalysisJob
from utils.file_handling.file_parser.json_parser import (
    get_full_json_as_dict_recursively,
)


class TagFrequencyJsonAnalysis(IAnalysisJob):
    """
    A job to analyze JSON keys and values, their frequency, and average content length across all JSON files.
    This job processes all JSON files in the given query's data directory, computes statistics
    on key and value usage, including nested keys, and saves the results to a CSV file.
    """

    def __init__(self, query_name: str):
        super().__init__("tag_frequency_json_analysis", query_name)
        self.tag_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"occurrences": 0, "total_length": 0, "max_length": 0}
        )

    def run(self) -> None:
        for idx, json_dict in enumerate(
            get_full_json_as_dict_recursively(self.data_path)
        ):
            self.process_dict(json_dict)
            if idx % 50_000 == 0:
                print(f"Processed {idx} files")

        for tag_stats in self.tag_stats.values():
            tag_stats["avg_length"] = (
                tag_stats["total_length"] / tag_stats["occurrences"]
                if tag_stats["occurrences"] > 0
                else 0
            )

    def process_dict(self, json_dict: Dict[str, Any], parent_key: str = "") -> None:
        for key, value in json_dict.items():
            current_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, dict):
                # Recursively process nested dictionaries
                self.process_dict(value, current_key)
            elif isinstance(value, list):
                # Process lists
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self.process_dict(item, f"{current_key}[{i}]")
                    else:
                        self.update_tag_stats(f"{current_key}[{i}]", str(item))
            else:
                # Process simple key-value pairs
                self.update_tag_stats(current_key, str(value))

    def update_tag_stats(self, tag: str, content: str) -> None:
        stats = self.tag_stats[tag]
        stats["occurrences"] += 1
        content_length = len(content)
        stats["total_length"] += content_length
        stats["max_length"] = max(stats["max_length"], content_length)

    def save_output(self) -> None:
        output_file = self.output_path / f"{self.analysis_name}_results.csv"
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Key Path", "Occurrences", "Average Length", "Max Length"])
            for tag, stats in sorted(self.tag_stats.items()):
                writer.writerow(
                    [
                        tag,
                        stats["occurrences"],
                        f"{stats['avg_length']:.2f}",
                        stats["max_length"],
                    ]
                )
        print(f"Results saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run JSON Tag Analysis")
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="Choose query to run analysis on",
    )
    args = parser.parse_args()
    job = TagFrequencyJsonAnalysis(args.query)
    job.run()
    job.save_output()
