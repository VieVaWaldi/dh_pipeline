from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from etl.cordis_extractor import CordisExtractor
from etl.cordis_transformer import CordisTransformer
from etl.sanitizer import DataSanitizer


def create_db_session() -> sessionmaker:
    """Create and return a database session factory."""
    database_url = (
        "postgresql://"
        "lu72hip@localhost:5433/"  # Keep localhost but
        "test_digicher"       # specify database name
    )
    
    try:
        engine = create_engine(
            database_url,
            connect_args={
                "connect_timeout": 5,
            }
        )
            
        return sessionmaker(bind=engine)
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        raise

def run_cordis_etl(source_path: Path, batch_size: int):
    session_factory = create_db_session()
    with session_factory() as session:
        print("Starting document processing...")
        doc_count = 0

        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            cordis_project = CordisExtractor().extract(document)

            try:
                if cordis_project.id_original:
                    CordisTransformer(session, DataSanitizer()).map_to_orm(cordis_project)
                    doc_count += 1

                if doc_idx % batch_size == 0:
                    session.commit()
                    print("Commit successful")

                if doc_idx % 1000 == 0:
                    print(f"Processed {doc_idx} document")


            except Exception as e:
                session.rollback()
                print(f"Error ingesting batch at document {doc_idx}:")
                print(f"Error details: {str(e)}")
                print(f"Document path: {path}")
                raise


if __name__ == "__main__":
    cordis_path = Path(
        "/vast/lu72hip/data/pile/extractors/cordis_culturalORheritage"
    )
    run_cordis_etl(cordis_path, batch_size=100)
