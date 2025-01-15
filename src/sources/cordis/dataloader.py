import logging
from pathlib import Path

from utils.config.config_loader import get_config
from core.file_handling.file_handling import get_root_path
from core.file_handling.file_parsing.general_parser import yield_all_documents
from utils.logging.logger import setup_logging
from sources.cordis.object_transformer import CordisTransformObj
from sources.cordis.orm_transformer import CordisTransformOrm
from core.dataloader.create_db_session import create_db_session
from core.dataloader.get_or_create import ModelCreationMonitor
from core.sanitizers.sanitizer import DataSanitizer


def run_cordis_dataloader(source_path: Path, batch_size: int):
    session_factory = create_db_session()
    with session_factory() as session:
        logging.info("Starting document processing...")
        doc_count = 0

        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            cordis_project = CordisTransformObj().get(document)

            try:
                if cordis_project.id_original:
                    CordisTransformOrm(session, DataSanitizer()).map_to_orm(
                        cordis_project
                    )
                    doc_count += 1

                if doc_idx % batch_size == 0:
                    session.commit()
                    logging.info("Commit successful")

                if doc_idx % 1000 == 0:
                    logging.info(f"Processed {doc_idx} document")

            except Exception as e:
                session.rollback()
                logging.error(f"Error ingesting batch at document {doc_idx}:")
                logging.error(f"Error details: {str(e)}")
                logging.error(f"Document path: {path}")
                raise
        session.commit()  # Dont forget to do that for the final smaller batch
    ModelCreationMonitor.log_stats()


if __name__ == "__main__":
    config = get_config()
    logging_path: Path = (
        get_root_path() / config["logging_path"] / "dataloader" / "cordis"
    )
    setup_logging(logging_path, "dataloader")

    cordis_path = Path("/vast/lu72hip/data/pile/extractors/cordis_culturalORheritage/")
    run_cordis_dataloader(cordis_path, batch_size=100)
