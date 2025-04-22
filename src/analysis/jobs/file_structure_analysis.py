import argparse
import csv
import logging
import os
from pathlib import Path
from typing import Dict

from analysis.utils.analysis_interface import IAnalysisJob


def get_file_size(file_path: Path) -> int:
    """
    Get the size of a file in bytes.
    """
    try:
        return os.path.getsize(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file at {file_path} does not exist.")
    except PermissionError:
        raise PermissionError(f"Permission denied when trying to access {file_path}.")
    except Exception as e:
        raise Exception(
            f"An error occurred when getting the size of {file_path}: {str(e)}"
        )


class FileStructureAnalysis(IAnalysisJob):
    """
    A job to compute and save file structure statistics for a given query's data.
    This job analyzes the directory structure and files associated with a specific query.
    The job computes the following statistics:
    - Total number of files and size
    - Number of files and size for each file type
    - Number of datasets with attachments
    - For each checkpoint:
        - Number of files
        - Total size of files
    All sizes are reported in gigabytes (GB).
    """

    def __init__(self, query_name: str):
        super().__init__("file_structure_analysis", query_name)
        self.stats: Dict[str, Dict[str, float]] = {}

    def run(self) -> None:
        logging.info(f"Running file structure analysis for query: {self.analysis_name}")
        self.stats["all_files"] = {"files": 0, "size": 0}
        self.stats["datasets_with_attachments"] = {"files": 0, "size": 0}

        for checkpoint in os.listdir(self.data_path):
            checkpoint_path = self.data_path / checkpoint
            if checkpoint_path.is_dir():
                self.stats[checkpoint] = {"files": 0, "size": 0}
                for root, dirs, files in os.walk(checkpoint_path):
                    for file in files:
                        file_path = Path(root) / file
                        file_size = get_file_size(file_path)
                        file_size_gb = file_size / (1024 * 1024 * 1024)  # Convert to GB

                        self.stats["all_files"]["files"] += 1
                        self.stats["all_files"]["size"] += file_size_gb

                        self.stats[checkpoint]["files"] += 1
                        self.stats[checkpoint]["size"] += file_size_gb

                        file_ext = file_path.suffix
                        if file_ext not in self.stats:
                            self.stats[file_ext] = {"files": 0, "size": 0}
                        self.stats[file_ext]["files"] += 1
                        self.stats[file_ext]["size"] += file_size_gb

                    if "attachments" in dirs:
                        self.stats["datasets_with_attachments"]["files"] += 1

        logging.info("File structure analysis completed")

    def save_output(self) -> None:
        logging.info("Saving file structure analysis results")
        csv_output_path = self.analysis_output_path / f"{self.analysis_name}.csv"
        with open(csv_output_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["Name", "Number of Files", "Size in Gigabyte"])

            writer.writerow(
                [
                    "all_files",
                    self.stats["all_files"]["files"],
                    f"{self.stats['all_files']['size']:.2f}",
                ]
            )

            for file_type in sorted(
                [k for k in self.stats.keys() if k.startswith(".")]
            ):
                writer.writerow(
                    [
                        file_type,
                        self.stats[file_type]["files"],
                        f"{self.stats[file_type]['size']:.2f}",
                    ]
                )

            writer.writerow(
                [
                    "datasets_with_attachments",
                    self.stats["datasets_with_attachments"]["files"],
                    "",
                ]
            )

            for checkpoint in sorted(
                [k for k in self.stats.keys() if k.startswith("last_")]
            ):
                writer.writerow(
                    [
                        checkpoint,
                        self.stats[checkpoint]["files"],
                        f"{self.stats[checkpoint]['size']:.2f}",
                    ]
                )

        logging.info(f"Results saved to {csv_output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run File Structure Analysis")
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="Choose query to run analysis on",
    )
    args = parser.parse_args()
    job = FileStructureAnalysis("arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB")#args.query)
    job.run()
    job.save_output()
