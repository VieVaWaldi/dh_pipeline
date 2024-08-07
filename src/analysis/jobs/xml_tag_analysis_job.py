import csv
from collections import defaultdict
from typing import Dict, Any, List

from analysis.jobs.analysis_job_interface import IAnalysisJob
from utils.file_handling.file_parser.xml_parser import get_full_xml_as_dict_recursively


class XMLTagAnalysisJob(IAnalysisJob):
    """
    A job to analyze XML tags, their frequency, and average content length across all XML files.
    This job processes all XML files in the given query's data directory, computes statistics
    on tag usage, including nested tags, and saves the results to a CSV file.
    The job focuses only on structural tags, ignoring special xmltodict attributes and text nodes.
    """

    def __init__(self, query_name: str):
        super().__init__("xml_tag_analysis", query_name)
        self.tag_stats: Dict[str, Dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

    def run(self) -> None:
        for xml_dict in get_full_xml_as_dict_recursively(self.data_path):
            self.process_dict(xml_dict)

        for tag_stats in self.tag_stats.values():
            tag_stats["avg_length"] = (
                tag_stats["total_length"] / tag_stats["occurrences"]
                if tag_stats["occurrences"] > 0
                else 0
            )

    def process_dict(self, data: Dict[str, Any], parent_tags: List[str] = []) -> None:
        for key, value in data.items():
            # Ignore special xmltodict keys
            if key.startswith("@") or key.startswith("#"):
                continue

            current_tag = ".".join(parent_tags + [key])

            self.tag_stats[current_tag]["occurrences"] += 1

            if isinstance(value, dict):
                self.process_dict(value, parent_tags + [key])
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self.process_dict(item, parent_tags + [key])
                    else:
                        # Count the length of non-dict items in lists
                        self.tag_stats[current_tag]["total_length"] += len(str(item))
            else:
                # Count the length of string values
                self.tag_stats[current_tag]["total_length"] += len(str(value))

    def save_output(self) -> None:
        output_file = self.output_path / f"{self.analysis_name}_results.csv"
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Tag", "Occurrences", "Average Length"])
            for tag, stats in sorted(self.tag_stats.items()):
                writer.writerow(
                    [
                        tag,
                        stats["occurrences"],
                        f"{stats['avg_length']:.2f}",
                    ]
                )
        print(f"Results saved to {output_file}")


if __name__ == "__main__":
    query_name = "cordis_contenttypeISprojectANDSTAR"
    job = XMLTagAnalysisJob(query_name)
    job.run()
    job.save_output()
