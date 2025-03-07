import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Type

from dotenv import load_dotenv

from core.etl.dataloader.create_db_session import create_db_session
from core.etl.transformer.get_or_create import ModelCreationMonitor
from core.file_handling.file_handling import (
    get_root_path,
)
from core.file_handling.file_parsing.general_parser import yield_all_documents
from interfaces.i_extractor import clean_extractor_name
from interfaces.i_object_transformer import IObjectTransformer
from interfaces.i_orm_transformer import IORMTransformer
from sources.cordis.object_transformer import CordisObjectTransformer
from sources.cordis.orm_transformer import CordisORMTransformer
from sources.openaire.object_transformer import OpenaireObjectTransformer
from sources.openaire.orm_transformer import OpenaireORMTransformer
from utils.config.config_loader import get_config, get_query_config
from utils.logger.logger import setup_logging


@dataclass
class SourceConfig:
    name: str
    object_transformer: Type[IObjectTransformer]
    orm_transformer: Type[IORMTransformer]
    source_path: Path
    batch_size: int = 100


def run_dataloader(source_config: SourceConfig):
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
            transformer = source_config.object_transformer(path)
            data_object, do_use = transformer.transform(document)

            try:
                if do_use:
                    source_config.orm_transformer(session).map_to_orm(data_object)
                    doc_count += 1

                # if doc_idx % source_config.batch_size == 0:
                #     session.commit()
                #     logging.info("Commit successful")

                if doc_idx % 1000 == 0:
                    logging.info(f"Processed {doc_idx} documents")

            except Exception as e:
                session.rollback()
                logging.error(f"Error ingesting batch at document {doc_idx}:")
                logging.error(f"Error details: {str(e)}")
                logging.error(f"Document path: {path}")
                raise
        # Also commit for leftovers
        # session.commit()
    ModelCreationMonitor.log_stats()
    log_run_time(start_time)


def log_run_time(start_time: datetime):
    end_time = datetime.now()
    duration = end_time - start_time
    hours = duration.total_seconds() / 3600
    minutes = (duration.total_seconds() % 3600) / 60

    logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m")


if __name__ == "__main__":
    source = "openaire"
    run = 0

    load_dotenv()
    config = get_config()

    data_path = Path(config["data_path"]) / (
        source
        + "_"
        + clean_extractor_name(get_query_config()["openaire"]["runs"][run]["query"])
    )
    if os.getenv("ENV") == "dev":
        data_path = get_root_path() / data_path

    source_configs = {
        "cordis": SourceConfig(
            name="cordis",
            object_transformer=CordisObjectTransformer,
            orm_transformer=CordisORMTransformer,
            source_path=data_path,
        ),
        "openaire": SourceConfig(
            name="openaire",
            object_transformer=OpenaireObjectTransformer,
            orm_transformer=OpenaireORMTransformer,
            source_path=data_path,
        ),
        # "arxiv": SourceConfig(
        #     name="arxiv",
        #     object_transformer=ArxivObjectTransformer,
        #     orm_transformer=ArxivORMTransformer,
        #     source_path=Path(config["sources"]["arxiv"]["path"]),
        # ),
    }

    logging_path = get_root_path() / config["logging_path"] / "dataloader" / source
    setup_logging(logging_path, "dataloader")

    run_dataloader(source_configs[source])
