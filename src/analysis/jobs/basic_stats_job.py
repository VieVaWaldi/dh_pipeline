import argparse
import csv
import logging
import os
from pathlib import Path
from typing import Dict

from analysis.jobs.analysis_job_interface import IAnalysisJob
from utils.file_handling.file_handling import get_file_size


class BasicStatsJob(IAnalysisJob):
    """
    A job to compute and save basic statistics for a given query's data.
    This job analyzes the directory structure and files associated with a specific query.

    The job computes the following statistics:
    - Total number of files
    - Total size of all files (in bytes and formatted as MB)
    - Distribution of file types (based on file extensions)
    - Number of datasets with attachments
    - For each checkpoint:
        - Number of files
        - Total size of files
    """

    def __init__(self, query_name: str):
        super().__init__("basic_stats", query_name)
        self.stats: Dict[str, any] = {}

    def run(self) -> None:
        logging.info(f"Running basic stats analysis for query: {self.analysis_name}")

        self.stats["total_files"] = 0
        self.stats["total_size"] = 0
        self.stats["file_types"] = {}
        self.stats["checkpoints"] = {}
        self.stats["datasets_with_attachments"] = 0

        for checkpoint in os.listdir(self.data_path):
            checkpoint_path = self.data_path / checkpoint
            if checkpoint_path.is_dir():
                self.stats["checkpoints"][checkpoint] = {"files": 0, "size": 0}

                for root, dirs, files in os.walk(checkpoint_path):
                    for file in files:
                        file_path = Path(root) / file
                        file_size = get_file_size(file_path)

                        self.stats["total_files"] += 1
                        self.stats["total_size"] += file_size
                        self.stats["checkpoints"][checkpoint]["files"] += 1
                        self.stats["checkpoints"][checkpoint]["size"] += file_size

                        file_ext = file_path.suffix
                        self.stats["file_types"][file_ext] = (
                            self.stats["file_types"].get(file_ext, 0) + 1
                        )

                        if "attachments" in dirs:
                            self.stats["datasets_with_attachments"] += 1
                            break  # Count each dataset with attachments only once

        self.stats["total_size_formatted"] = (
            f"{self.stats['total_size'] / (1024 * 1024):.2f} MB"
        )

        logging.info("Basic stats analysis completed")

    def save_output(self) -> None:
        logging.info("Saving basic stats analysis results")

        # Save as CSV
        csv_output_path = self.output_path / f"{self.analysis_name}.csv"
        with open(csv_output_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["Metric", "Value"])
            for key, value in self.stats.items():
                if isinstance(value, dict):
                    writer.writerow([key, ""])
                    for sub_key, sub_value in value.items():
                        writer.writerow([f"  {sub_key}", sub_value])
                else:
                    writer.writerow([key, value])

        logging.info(f"Results saved to {csv_output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Basic Stats Analysis")
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="Choose query to run analysis on",
    )
    args = parser.parse_args()

    job = BasicStatsJob(args.query)
    job.run()
    job.save_output()
