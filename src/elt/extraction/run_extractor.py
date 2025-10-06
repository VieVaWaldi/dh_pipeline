import argparse
import logging
from datetime import datetime
from typing import Type

from dotenv import load_dotenv

from interfaces.i_extractor import IExtractor, ExtractorConfig
from sources.arxiv.extractor import ArxivExtractor
from sources.cordis.extractor import CordisExtractor
# from sources.coreac.extractor import CoreacExtractor
from sources.openaire.extractor import OpenAIREExtractor
from utils.config.config_loader import get_query_config
from utils.error_handling.error_handling import log_and_exit
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time


def run_extractor(config: ExtractorConfig, extractor_class: Type[IExtractor]):
    """Main extraction runner that handles the extraction loop."""
    start_time = datetime.now()
    logging.info(
        f"Starting extraction for {config.name}, query_id: {config.query_id} \
        \n\t- with query: {config.query} \
        \n\t- with checkpoint: {config.checkpoint_name} \
        \n\t- with range: {config.checkpoint_range} \
        \n\t- with download attachment: {config.download_attachments}"
    )

    continue_extraction = True
    while continue_extraction:
        try:
            extractor = extractor_class(extractor_config)
            continue_extraction = extractor.extract_until_next_checkpoint()
        except Exception as e:
            log_and_exit(f"Error during extraction iteration", e)

    log_run_time(start_time)
    logging.info(f"Successfully completed extraction for {config.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extractor Runner")
    parser.add_argument("--source", help="Select data source", required=True)
    parser.add_argument(
        "--query_id",
        help="Query ID of the source (select from /config_queries)",
        required=True,
        type=int,
    )
    args = parser.parse_args()

    load_dotenv()
    setup_logging("extractor", f"{args.source}-query_id-{args.query_id}")

    source_config = get_query_config()[args.source]
    query_config = source_config["queries"][args.query_id]

    extractor_classes = {
        "arxiv": ArxivExtractor,
        "cordis": CordisExtractor,
        # "coreac": CoreacExtractor,
        "openaire": OpenAIREExtractor,
    }

    extractor_config = ExtractorConfig(
        name=args.source,
        query=query_config["query"],
        query_id=args.query_id,
        checkpoint_name=source_config["checkpoint"],
        checkpoint_start=query_config["checkpoint_start"],
        checkpoint_range=query_config["checkpoint_range"],
        download_attachments=query_config["download_attachments"],
    )

    run_extractor(extractor_config, extractor_classes[args.source])
