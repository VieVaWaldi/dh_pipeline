import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Type

from dotenv import load_dotenv

from elt.extraction.extractor_utils import clean_extractor_name
from interfaces.i_extractor import IExtractor
from lib.file_handling.file_utils import get_project_root_path
from sources.arxiv.extractor import ArxivExtractor
from utils.config.config_loader import get_config, get_query_config
from utils.logger.logger import setup_logging


# from sources.cordis.extractor import CordisExtractor
# from sources.coreac.extractor import CoreacExtractor
# from sources.openaire.extractor import OpenaireExtractor


@dataclass
class ExtractorConfig:
    name: str
    extractor_class: Type[IExtractor]
    query: str
    checkpoint_name: str
    checkpoint_range: str
    download_attachments: bool
    run_id: int


def run_extractor(config: ExtractorConfig):
    """Main extraction runner that handles the extraction loop."""
    start_time = datetime.now()
    logging.info(f"Starting extraction for {config.name}")

    extractor_name = clean_extractor_name(
        f"{config.name}_run-{config.run_id}_{config.query}"
    )
    extractor = config.extractor_class(
        extractor_name=extractor_name,
        checkpoint_name=config.checkpoint_name,
        checkpoint_range=config.checkpoint_range,
        query=config.query,
        download_attachments=config.download_attachments,
    )

    # query = (run_config["query"],)
    # checkpoint_name = (source_config["checkpoint"],)
    # checkpoint_range = (run_config["checkpoint_range"],)

    iteration_count = 0
    continue_extraction = True

    while continue_extraction:
        iteration_count += 1
        logging.info(
            f"Starting extraction iteration #{iteration_count} for {extractor_name}"
        )

        try:
            continue_extraction = extractor.extract_until_next_checkpoint()
            if not continue_extraction:
                logging.info(f"Extraction completed after {iteration_count} iterations")

        except Exception as e:
            logging.error(
                f"Error during extraction iteration {iteration_count}: {str(e)}"
            )
            raise

    log_run_time(start_time)
    logging.info(f"Successfully completed extraction for {config.name}")


# ToDo: Make utils and also use for loader
def log_run_time(start_time: datetime):
    duration = datetime.now() - start_time
    hours = duration.total_seconds() / 3600
    minutes = (duration.total_seconds() % 3600) / 60
    logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m")


if __name__ == "__main__":

    # ToDo: REMOVE
    import sys
    dev_args = ["--source", "arxiv", "--run", "0"]
    sys.argv.extend(dev_args)

    parser = argparse.ArgumentParser(description="Extractor Runner")
    parser.add_argument("--source", help="Select data source", required=True)
    parser.add_argument("--run", help="Run ID of the source", required=True, type=int)
    args = parser.parse_args()

    load_dotenv()
    config = get_config()
    query_config = get_query_config()

    source_config = query_config[args.source]
    run_config = source_config["runs"][args.run]

    extractor_classes = {
        "arxiv": ArxivExtractor,
        # "cordis": CordisExtractor,
        # "coreac": CoreacExtractor,
        # "openaire": OpenaireExtractor,
    }

    extractor_config = ExtractorConfig(
        name=args.source,
        extractor_class=extractor_classes[args.source],
        query=run_config["query"],
        checkpoint_name=source_config["checkpoint"],
        checkpoint_range=run_config["checkpoint_range"],
        download_attachments=run_config["download_attachments"],
        run_id=args.run,
    )

    # ToDo: Only enter folder AND name, eg extractor AND args.source ?
    logging_path = (
        get_project_root_path() / config["logging_path"] / "extractors" / args.source
    )
    setup_logging(logging_path, "extraction")

    run_extractor(extractor_config)
