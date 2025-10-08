import argparse
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple

from dotenv import load_dotenv

from analytics.sql.orm_raw_files import RawFiles
from lib.database.create_db_session import create_db_session
from lib.database.get_or_create import get_or_create
from utils.config.config_loader import get_config, get_project_root_path


def get_file_extension(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return ext if ext else "no_extension"


def analyze_checkpoint(checkpoint_path: Path) -> Tuple[Dict[str, int], int]:
    """
    Analyze a single checkpoint directory.

    Returns:
        Tuple of (file_type_counts, total_size_bytes)
    """
    file_counts = defaultdict(int)
    total_size = 0

    for record_dir in checkpoint_path.iterdir():
        if not record_dir.is_dir():
            continue

        for file_path in record_dir.rglob("*"):
            if file_path.is_file():
                ext = get_file_extension(file_path.name)
                file_counts[ext] += 1
                try:
                    total_size += file_path.stat().st_size
                except OSError:
                    pass
    return dict(file_counts), total_size


def analyze_source(source_path: Path) -> Dict:
    """Analyze all checkpoints in a source directory."""
    checkpoints = {}
    source_file_counts = defaultdict(int)
    source_total_size = 0

    for checkpoint_dir in sorted(source_path.iterdir()):
        if not checkpoint_dir.is_dir() or not checkpoint_dir.name.startswith("cp_"):
            continue

        file_counts, total_size = analyze_checkpoint(checkpoint_dir)

        checkpoints[checkpoint_dir.name] = {
            "file_types": file_counts,
            "disk_usage_bytes": total_size,
            "disk_usage_mb": total_size / (1024 * 1024),
        }

        # Aggregate to source level
        for ext, count in file_counts.items():
            source_file_counts[ext] += count
        source_total_size += total_size

    return {
        "checkpoints": checkpoints,
        "total_file_types": dict(source_file_counts),
        "total_disk_usage_bytes": source_total_size,
        "total_disk_usage_mb": source_total_size / (1024 * 1024),
        "total_disk_usage_gb": source_total_size / (1024**3),
    }


def run_raw_file_analysis():
    parser = argparse.ArgumentParser(description="Raw File Analytics")
    parser.add_argument("--source_query_id", help="Select source_query_id")
    args = parser.parse_args()

    load_dotenv()
    config = get_config()

    pile_path = Path(config["data_path"])
    if os.getenv("ENV") == "dev":
        pile_path = get_project_root_path() / pile_path

    results = {}

    for source_dir in sorted(pile_path.iterdir()):
        if not source_dir.is_dir():
            continue
        if args.source_query_id and args.source_query_id != source_dir.name:
            continue
        results[source_dir.name] = analyze_source(source_dir)

    with create_db_session()() as session:
        for source_name, data in results.items():
            get_or_create(session, RawFiles,{"source_query_id": source_name},
                          total_disk_usage_gb=data['total_disk_usage_gb'],
                          file_types_total=data["total_file_types"],
                          checkpoints=data["checkpoints"])
        session.commit()


if __name__ == "__main__":
    run_raw_file_analysis()
