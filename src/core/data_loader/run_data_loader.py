import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Type

from dotenv import load_dotenv

from common_utils.config.config_loader import get_config, get_source_data_path
from common_utils.logger.logger import setup_logging
from core.data_loader.checkpoint_manager import CheckpointManager
from core.data_loader.utils.create_db_session import create_db_session
from core.data_loader.utils.get_or_create import ModelCreationMonitor
from core.file_handling.file_utils import (
    get_project_root_path,
)
from core.file_handling.file_walker import yield_all_documents
from sources.arxiv.data_loader import ArxivDataLoader
from sources.cordis.data_loader import CordisDataLoader
from sources.coreac.data_loader import IDataLoader, CoreacDataLoader


@dataclass
class SourceConfig:
    name: str
    data_loader: Type[IDataLoader]
    source_path: Path
    batch_size: int = 100


def run_data_loader(source_config: SourceConfig):
    start_time = datetime.now()
    session_factory = create_db_session()
    cp = CheckpointManager(source_config.name)

    doc_count = 0
    skip_count = 0
    commit_count = 0

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

            try:
                data_loader = source_config.data_loader(path)
                data_loader.load(session, document)

                if doc_idx % source_config.batch_size == 0:
                    session.commit()
                    commit_count += 1
                    logging.info(f"Commit # {commit_count} successful")

                if doc_idx % 1000 == 0:
                    logging.info(f"Processed # {doc_idx} documents")

            except Exception as e:
                session.rollback()
                logging.error(f"Error ingesting batch at document {doc_idx}:")
                logging.error(f"Error details: {str(e)}")
                logging.error(f"Document path: {path}")
                raise

        session.commit()  # Also commit for leftovers
        cp.update_cp()  # Only update at the end, because files might not be ordered

    log_run_time(start_time)
    ModelCreationMonitor.log_stats()
    validate(source_config.name, doc_count, skip_count)


def validate(source_name: str, doc_count: int, skip_count: int):
    logging.info(
        f"{source_name}: Files processed {doc_count} and files skipped {skip_count}, used {doc_count-skip_count} files."
    )
    # ToDo: Validate that row count = skipped + not skipped for all sources


def log_run_time(start_time: datetime):
    end_time = datetime.now()
    duration = end_time - start_time
    hours = duration.total_seconds() / 3600
    minutes = (duration.total_seconds() % 3600) / 60
    logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m")


if __name__ == "__main__":
    source_name = "arxiv"  # get from args
    load_dotenv()
    config = get_config()
    data_path = get_source_data_path(source_name, config, run=0)

    source_configs = {
        "arxiv": SourceConfig(
            name="arxiv",
            data_loader=ArxivDataLoader,
            source_path=Path(data_path),
        ),
        "coreac": SourceConfig(
            name="coreac",
            data_loader=CoreacDataLoader,
            source_path=Path(data_path),
        ),
        "cordis": SourceConfig(
            name="cordis",
            data_loader=CordisDataLoader,
            source_path=data_path,
        ),
        # "openaire": SourceConfig(
        #     name="openaire",
        #     source_path=data_path,
        # ),
    }

    logging_path = (
        get_project_root_path() / config["logging_path"] / "data_loader" / source_name
    )
    setup_logging(logging_path, "data_loader")

    run_data_loader(source_configs[source_name])
