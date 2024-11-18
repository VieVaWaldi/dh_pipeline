from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from etl.arxiv_extractor import ArxivExtractor
from etl.arxiv_transfomer import ArxivTransformer
from etl.sanitizer import DataSanitizer


def run_arxiv_etl(source_path: Path, batch_size: int):
    session_factory = create_db_session()
    with session_factory() as session:

        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            arxiv_entry = ArxivExtractor().extract(document)

            try:
                ArxivTransformer(session, DataSanitizer()).map_to_orm(arxiv_entry)

                if doc_idx % batch_size == 0:
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
    arxiv_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )
    run_arxiv_etl(arxiv_path, batch_size=10)
