import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Type

from dotenv import load_dotenv

from elt.loading.checkpoint_manager import CheckpointManager
from interfaces.i_loader import ILoader
from lib.database.create_db_session import create_db_session
from lib.database.get_or_create import ModelCreationMonitor
from lib.file_handling.path_utils import get_source_data_path
from lib.file_handling.yield_documents import yield_all_documents
from sources.arxiv.loader import ArxivLoader
from sources.cordis.loader import CordisLoader
from sources.coreac.loader import CoreacLoader
from sources.openaire.loader import OpenaireLoader
from utils.config.config_loader import get_query_config
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time


@dataclass
class LoaderConfig:
    name: str
    loader_class: Type[ILoader]
    source_path: Path
    query_id: int
    batch_size: int = 100


def run_loader(config: LoaderConfig):
    start_time = datetime.now()
    logging.info(
        f"Starting loading for {config.name}, query_id: {config.query_id}\
        \n\t- for query: {get_query_config()[config.name]['queries'][config.query_id]['query']}\
        \n\t- at path: {config.source_path}"
    )

    cp = CheckpointManager(config.name, config.query_id)
    doc_count = 0
    skip_count = 0

    with create_db_session()() as session:
        for doc_idx, (document, path, mtime) in enumerate(
            yield_all_documents(config.source_path)
        ):
            doc_count += 1
            if cp.should_skip_or_store(mtime):
                skip_count += 1
                continue

            try:
                loader = config.loader_class(path)
                loader.load(session, document)

                if doc_idx % config.batch_size == 0:
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
        cp.update_cp()  # Only update cp at the end, because files are not ordered

    ModelCreationMonitor.log_stats()
    validate(config.name, doc_count, skip_count)
    log_run_time(start_time)


def validate(source_name: str, doc_count: int, skip_count: int):
    logging.info(
        f"{source_name}: Files processed {doc_count} and files skipped {skip_count}, used {doc_count-skip_count} files."
    )
    # ToDo: Monitor OCR converter, skips and processed
    # ToDo: Validate, that row count = skipped + not skipped for all sources


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Loader Runner")
    parser.add_argument("--source", help="Select data source", required=True)
    parser.add_argument(
        "--query_id",
        help="Query ID of the source (select from /config_queries)",
        required=True,
        type=int,
    )
    args = parser.parse_args()

    load_dotenv()
    setup_logging("loader", f"{args.source}-query_id-{args.query_id}")

    loader_classes = {
        "arxiv": ArxivLoader,
        "cordis": CordisLoader,
        "coreac": CoreacLoader,
        "openaire": OpenaireLoader,
    }

    loader_config = LoaderConfig(
        name=args.source,
        loader_class=loader_classes[args.source],
        source_path=get_source_data_path(args.source, query_id=args.query_id),
        query_id=args.query_id,
    )

    run_loader(loader_config)
