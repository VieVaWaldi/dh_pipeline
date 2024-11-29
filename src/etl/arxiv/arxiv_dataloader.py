from pathlib import Path

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from etl.arxiv.arxiv_transform_obj import ArxivTransformObj
from etl.arxiv.arxiv_transform_orm import ArxivTransformOrm
from etl.utils.database.db_connection import create_db_session
from etl.utils.sanitizer import DataSanitizer


def run_arxiv_dataloader(source_path: Path, batch_size: int):
    session_factory = create_db_session()
    with session_factory() as session:

        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            arxiv_entry = ArxivTransformObj().extract(document)

            try:
                ArxivTransformOrm(session, DataSanitizer()).map_to_orm(arxiv_entry)

                if doc_idx % batch_size == 0:
                    session.commit()
            except Exception as e:
                session.rollback()
                print(f"Error ingesting batch: {str(e)}")
                raise


if __name__ == "__main__":
    arxiv_path = Path(
        "/vast/lu72hip/data/pile/extractors/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )
    run_arxiv_dataloader(arxiv_path, batch_size=10)
