import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Type

from dotenv import load_dotenv

from core.etl.data_loader.utils.create_db_session import create_db_session
from core.etl.data_loader.utils.get_or_create import ModelCreationMonitor
from core.file_handling.file_handling import (
    get_root_path,
)
from core.file_handling.general_parser import yield_all_documents
from sources.coreac.data_loader import IDataLoader, CoreacDataLoader
from common_utils.config.config_loader import get_config, get_data_path
from common_utils.logger.logger import setup_logging


@dataclass
class SourceConfig:
    name: str
    data_loader: Type[IDataLoader]
    source_path: Path
    batch_size: int = 100


def run_data_loader(source_config: SourceConfig):
    start_time = datetime.now()
    session_factory = create_db_session()
    with session_factory() as session:
        logging.info(
            f"Starting document processing for {source_config.name} with path {source_config.source_path}"
        )

        doc_count = 0
        for doc_idx, (document, path) in enumerate(
            yield_all_documents(source_config.source_path)
        ):
            if path.parent.name == "test_data":
                a = 1
            try:
                data_loader = source_config.data_loader(path)
                data_loader.load(session, document)
                doc_count += 1

                if doc_idx % source_config.batch_size == 0:
                    session.commit()
                    logging.info("Commit successful")

                if doc_idx % 1000 == 0:
                    logging.info(f"Processed {doc_idx} documents")

            except Exception as e:
                session.rollback()
                logging.error(f"Error ingesting batch at document {doc_idx}:")
                logging.error(f"Error details: {str(e)}")
                logging.error(f"Document path: {path}")
                raise
        # Also commit for leftovers
        session.commit()
    ModelCreationMonitor.log_stats()
    log_run_time(start_time)


def log_run_time(start_time: datetime):
    end_time = datetime.now()
    duration = end_time - start_time
    hours = duration.total_seconds() / 3600
    minutes = (duration.total_seconds() % 3600) / 60
    logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m")


if __name__ == "__main__":
    source_name = "coreac"
    load_dotenv()
    config = get_config()
    data_path = get_data_path(source_name, config, run=0)

    source_configs = {
        # "cordis": SourceConfig(
        #     name="cordis",
        #     source_path=data_path,
        # ),
        # "openaire": SourceConfig(
        #     name="openaire",
        #     source_path=data_path,
        # ),
        # "arxiv": SourceConfig(
        #     name="arxiv",
        #     source_path=Path(config["sources"]["arxiv"]["path"]),
        # ),
        "coreac": SourceConfig(
            name="coreac",
            data_loader=CoreacDataLoader,
            source_path=Path(data_path),
        ),
    }

    logging_path = get_root_path() / config["logging_path"] / "data_loader" / source_name
    setup_logging(logging_path, "data_loader")

    run_data_loader(source_configs[source_name])
