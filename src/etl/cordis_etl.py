from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from etl.cordis_extractor import CordisExtractor
from etl.cordis_transformer import CordisTransformer
from etl.sanitizer import DataSanitizer


def run_cordis_etl(source_path: Path, batch_size: int):
    session_factory = create_db_session()
    with session_factory() as session:

        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            cordis_project = CordisExtractor().extract(document)

            try:
                CordisTransformer(session, DataSanitizer()).map_to_orm(cordis_project)

                if doc_idx % batch_size == 0:
                    print(f"Created: {doc_idx} entities.")
                    session.commit()
            except Exception as e:
                session.rollback()
                print(f"Error ingesting batch: {str(e)}")
                raise


def create_db_session() -> sessionmaker:
    """Create and return a database session factory."""
    # ToDo: Get from .env config
    database_url = "postgresql://wehrenberger@localhost:5432/test_digicher"
    engine = create_engine(database_url)
    return sessionmaker(bind=engine)


if __name__ == "__main__":
    cordis_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/cordis_culturalORheritage"
    )
    run_cordis_etl(cordis_path, batch_size=10)
