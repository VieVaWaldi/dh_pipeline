import logging
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from lib.database.create_db_session import create_db_session
from lib.database.get_or_create import get_or_create, ModelCreationMonitor
from lib.file_handling.file_utils import get_project_root_path
from lib.sanitizers.sanitizer import (
    parse_names_and_identifiers,
    parse_content,
    parse_float,
)
from sources.meta_heritage.data_model import (
    Stakeholder,
    OrganizationType,
    CHTopic,
    JunctionStakeholderOrganizationType,
    JunctionStakeholderHeritageTopic,
)
from utils.config.config_loader import get_config, get_source_data_path
from utils.logger.logger import setup_logging


def extract_coordinates(
    shape_str: Optional[str],
) -> Tuple[Optional[float], Optional[float]]:
    """Extract longitude and latitude from POINT string format."""
    if not shape_str or not isinstance(shape_str, str):
        return None, None

    try:
        # Extract coordinates from "POINT (longitude latitude)" format
        coords_part = shape_str.replace("POINT (", "").replace(")", "")
        lon_str, lat_str = coords_part.split()
        longitude = parse_float(lon_str)
        latitude = parse_float(lat_str)
        return longitude, latitude
    except (ValueError, AttributeError):
        return None, None


def process_batch(session: Session, batch_df: DataFrame):
    for _, row in batch_df.iterrows():
        try:
            name = parse_names_and_identifiers(row.get("NAME"))
            if not name:
                logging.warning(f"Skipping row with missing name: {row.to_dict()}")
                continue

            longitude, latitude = extract_coordinates(row.get("SHAPE"))
            description = parse_content(row.get("BESCHREIBUNG"))

            stakeholder, created = get_or_create(
                session,
                Stakeholder,
                unique_key={"name": name},
                ownership="public",
                city="Vienna",
                country="Austria",
                latitude=latitude,
                longitude=longitude,
                data_source_type="government_open_data",
                data_source_name="data.gv.at",
            )

            if description and created:
                stakeholder.legal_status = description

            session.flush()

            glam_org_type, _ = get_or_create(
                session, OrganizationType, unique_key={"type_number": 1}
            )

            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "organization_type_id": glam_org_type.id,
                },
            )

            aristocratic_topic, _ = get_or_create(
                session, CHTopic, unique_key={"topic_number": 1}
            )

            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "heritage_topic_id": aristocratic_topic.id,
                },
            )

        except Exception as e:
            logging.error(f"Error processing castle {row.get('NAME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    logging.info(f"Starting to load data from {file_path}")
    df = pd.read_csv(file_path, sep=",")

    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]

        try:
            with create_db_session()() as session:

                logging.info(
                    f"Processing batch {i//batch_size + 1}: rows {i+1} to {min(i+batch_size, len(df))}"
                )

                process_batch(session, batch_df)
                session.commit()

        except Exception as e:
            logging.error(f"Error in batch {i//batch_size + 1}: {e}")
            session.rollback()


if __name__ == "__main__":
    files = ["Public/Austria/Austria data.gv.at/Castles Wien/Castles.csv"]
    source_name = "meta_heritage"
    log_name = "Austria_Castles"
    batch_size = 100

    config = get_config()
    logging_path = (
        get_project_root_path()
        / Path(config["logging_path"])
        / "loading"
        / "meta_heritage"
    )
    setup_logging(logging_path, log_name)

    for file in files:
        file_path = get_source_data_path(source_name, config, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")
