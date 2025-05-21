import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Type

from dotenv import load_dotenv

from utils.config.config_loader import get_config, get_source_data_path
from utils.logger.logger import setup_logging
from loader.checkpoint_manager import CheckpointManager
from lib.loader.create_db_session import create_db_session
from lib.loader.get_or_create import ModelCreationMonitor
from lib.file_handling.file_utils import (
    get_project_root_path,
)
from lib.file_handling.file_walker import yield_all_documents
from sources.arxiv.data_loader import ArxivDataLoader
from sources.cordis.data_loader import CordisDataLoader
from sources.coreac.data_loader import IDataLoader, CoreacDataLoader
from sources.openaire.data_loader import OpenaireDataLoader


@dataclass
class SourceConfig:
    name: str
    data_loader: Type[IDataLoader]
    source_path: Path
    run_id: int
    batch_size: int = 100


def run_data_loader(source_config: SourceConfig):
    start_time = datetime.now()
    session_factory = create_db_session()
    cp = CheckpointManager(source_config.name, source_config.run_id)

    doc_count = 0
    skip_count = 0

    with session_factory() as session:
        logging.info(
            f"Starting document processing for {source_config.name} with path {source_config.source_path}"
        )

        for doc_idx, (document, path, mtime) in enumerate(
            yield_all_documents(source_config.source_path)
        ):
            doc_count += 1
            if cp.should_skip_or_store(mtime):
                skip_count += 1
                continue

            # ToDo REMOVE!
            if source_config.name == "cordis" and doc_count < 10_100:
                continue
            if path == Path(
                "/vast/lu72hip/data/pile/cordis_culturalORheritage/last_startDate_2015-01-01/project-rcn-211925_en/project-rcn-211925_en.xml"
            ):
                logging.info(f"[skipping oom path] {path}")
                continue

            try:
                data_loader = source_config.data_loader(path)
                data_loader.load(session, document)

                if doc_idx % source_config.batch_size == 0:
                    session.commit()
                    session.expunge_all()  # Clear all entities from memory

                if doc_idx % 1000 == 0:
                    logging.info(f"Processed # {doc_idx} documents")

            except Exception as e:
                session.rollback()
                logging.error(f"Error ingesting batch at document {doc_idx}:")
                logging.error(f"Error details: {str(e)}")
                logging.error(f"Document path: {path}")
                raise

        session.commit()  # Also commit leftovers
        cp.update_cp()  # Only update at the end, because files are not ordered

    log_run_time(start_time)
    ModelCreationMonitor.log_stats()
    # ToDo: Monitor OCR converter, skips and processed
    validate(source_config.name, doc_count, skip_count)


def validate(source_name: str, doc_count: int, skip_count: int):
    logging.info(
        f"{source_name}: Files processed {doc_count} and files skipped {skip_count}, used {doc_count-skip_count} files."
    )
    # ToDo: Validate that row count = skipped + not skipped for all sources


def log_run_time(start_time: datetime):
    duration = datetime.now() - start_time
    hours = duration.total_seconds() / 3600
    minutes = (duration.total_seconds() % 3600) / 60
    logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m")


if __name__ == "__main__":
    import sys

    dev_args = ["--source", "cordis", "--run", "0"]
    sys.argv.extend(dev_args)

    parser = argparse.ArgumentParser(description="Data Loader Runner")
    parser.add_argument("--source", help="Select data source", required=True)
    parser.add_argument("--run", help="Run of the source", required=True, type=int)
    args = parser.parse_args()

    load_dotenv()
    config = get_config()
    data_path = get_source_data_path(args.source, config, run=args.run)

    source_configs = {
        "arxiv": SourceConfig(
            name="arxiv",
            data_loader=ArxivDataLoader,
            source_path=data_path,
            run_id=args.run,
        ),
        "coreac": SourceConfig(
            name="coreac",
            data_loader=CoreacDataLoader,
            source_path=data_path,
            run_id=args.run,
        ),
        "cordis": SourceConfig(
            name="cordis",
            data_loader=CordisDataLoader,
            source_path=data_path,
            run_id=args.run,
        ),
        "openaire": SourceConfig(
            name="openaire",
            data_loader=OpenaireDataLoader,
            source_path=data_path,
            run_id=args.run,
        ),
    }

    logging_path = (
        get_project_root_path() / config["logging_path"] / "loader" / args.source
    )
    setup_logging(logging_path, "loader")

    run_data_loader(source_configs[args.source])
