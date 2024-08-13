import argparse
import csv
from collections import defaultdict
from typing import Dict, Any

from analysis.jobs.analysis_interface import IAnalysisJob
from utils.file_handling.file_parser.xml_parser import get_full_xml_as_dict_recursively


class TagFrequencyXmlAnalysis(IAnalysisJob):
    """
    A job to analyze XML tags and attributes, their frequency, and average content length across all XML files.
    This job processes all XML files in the given query's data directory, computes statistics
    on tag and attribute usage, including nested tags, and saves the results to a CSV file.
    """

    def __init__(self, query_name: str):
        super().__init__("tag_frequency_xml_analysis", query_name)
        self.tag_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"occurrences": 0, "total_length": 0, "max_length": 0}
        )

    def run(self) -> None:
        for idx, xml_dict in enumerate(
            get_full_xml_as_dict_recursively(self.data_path)
        ):
            self.process_dict(xml_dict)
            if idx % 50_000 == 0:
                print(f"Processed {idx} files")

        for tag_stats in self.tag_stats.values():
            tag_stats["avg_length"] = (
                tag_stats["total_length"] / tag_stats["occurrences"]
                if tag_stats["occurrences"] > 0
                else 0
            )

    def process_dict(self, xml_dict: Dict[str, Any], parent_tag: str = "") -> None:
        for key, value in xml_dict.items():
            current_tag = f"{parent_tag}.{key}" if parent_tag else key

            if isinstance(value, dict):
                # Process attributes
                for attr_key, attr_value in value.items():
                    if attr_key.startswith("@"):
                        attr_tag = f"{current_tag}@{attr_key[1:]}"
                        self.update_tag_stats(attr_tag, str(attr_value))

                # Process text content
                if "#text" in value:
                    self.update_tag_stats(current_tag, str(value["#text"]))

                # Recursively process nested elements
                self.process_dict(value, current_tag)
            else:
                # Process simple tag content
                self.update_tag_stats(current_tag, str(value))

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
            writer.writerow(["Tag", "Occurrences", "Average Length", "Max Length"])
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
    parser = argparse.ArgumentParser(description="Run XML Tag Analysis")
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="Choose query to run analysis on",
    )
    args = parser.parse_args()
    job = TagFrequencyXmlAnalysis(args.query)
    job.run()
    job.save_output()
