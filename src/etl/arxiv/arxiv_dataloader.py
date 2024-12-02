import logging
from pathlib import Path

from common_utils.config.config_loader import get_config
from common_utils.file_handling.file_handling import (
    find_pdfs_in_directory,
    get_root_path,
)
from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from common_utils.logging.logger import setup_logging
from etl.arxiv.arxiv_transform_obj import ArxivTransformObj
from etl.arxiv.arxiv_transform_orm import ArxivTransformOrm
from etl.utils.database.db_connection import create_db_session
from etl.utils.database.get_or_create import ModelCreationMonitor
from etl.utils.sanitizer import DataSanitizer


def run_arxiv_dataloader(source_path: Path, batch_size: int, dry_run: bool = True):

    tmp_pdf_info = {"Was None": 0, "Was 1": 0, "Was more than 1": 0}

    session_factory = create_db_session()
    with session_factory() as session:
        for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
            arxiv_entry = ArxivTransformObj().extract(document)

            pdf_files = find_pdfs_in_directory(path)
            if pdf_files is None or len(pdf_files) == 0:
                tmp_pdf_info["Was None"] += 1
            elif len(pdf_files) == 1:
                # pdf_text = pdf_to_text(pdf_files[0])
                tmp_pdf_info["Was 1"] += 1
            else:
                tmp_pdf_info["Was more than 1"] += 1

            try:
                ArxivTransformOrm(session, DataSanitizer()).map_to_orm(arxiv_entry)

                if doc_idx % batch_size == 0:
                    # Log statistics before potential commit
                    ModelCreationMonitor.log_stats()
                    if dry_run:
                        session.rollback()
                    else:
                        session.commit()

            except Exception as e:
                session.rollback()
                logging.info(f"Error ingesting batch: {str(e)}")
                raise

        if dry_run:
            session.rollback()
        else:
            session.commit()
    ModelCreationMonitor.log_stats()

    print("\n=== PDF File Statistics ===")
    print(f"PDFs missing:     {tmp_pdf_info['Was None']}")
    print(f"Single PDF:       {tmp_pdf_info['Was 1']}")
    print(f"Multiple PDFs:    {tmp_pdf_info['Was more than 1']}")
    print(f"Total processed:  {sum(tmp_pdf_info.values())}")


if __name__ == "__main__":
    config = get_config()
    logging_path: Path = (
        get_root_path() / config["logging_path"] / "dataloader" / "arxiv"
    )
    setup_logging(logging_path, "dataloader")

    arxiv_path = Path(
        "/vast/lu72hip/data/pile/extractors/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
        # "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )
    run_arxiv_dataloader(arxiv_path, batch_size=10, dry_run=True)
