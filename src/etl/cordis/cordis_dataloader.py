from pathlib import Path

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from etl.cordis.cordis_transform_obj import CordisTransformObj
from etl.cordis.cordis_transform_orm import CordisTransformOrm
from etl.utils.database.db_connection import create_db_session
from etl.utils.sanitizer import DataSanitizer


def run_cordis_dataloader(source_path: Path, batch_size: int):
    session_factory = create_db_session()
    with session_factory() as session:
        print("Starting document processing...")
        doc_count = 0

        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            cordis_project = CordisTransformObj().extract(document)

            try:
                if cordis_project.id_original:
                    CordisTransformOrm(session, DataSanitizer()).map_to_orm(
                        cordis_project
                    )
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
    cordis_path = Path("/vast/lu72hip/data/pile/extractors/cordis_culturalORheritage")
    run_cordis_dataloader(cordis_path, batch_size=100)

    # logging_path: Path = (
    #     get_root_path() / config["logging_path"] / "extractors" / extractor_name
    # )
    # ensure_path_exists(logging_path)
    # setup_logging(logging_path, "extractor")
