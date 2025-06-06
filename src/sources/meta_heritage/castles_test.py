import logging
from pathlib import Path
import re
from typing import Optional, Tuple

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from lib.database.create_db_session import create_db_session
from lib.database.get_or_create import get_or_create, ModelCreationMonitor
from lib.file_handling.path_utils import get_source_data_path
from lib.sanitizers.parse_primitives import parse_float
from lib.sanitizers.parse_text import parse_names_and_identifiers, parse_content
from sources.meta_heritage.orm_model import (
    Stakeholder,
    OrganizationType,
    CHTopic,
    JunctionStakeholderOrganizationType,
    JunctionStakeholderHeritageTopic,
)
from utils.logger.logger import setup_logging


def parse_coordinates_from_point(
    point_str: Optional[str],
) -> Tuple[Optional[float], Optional[float]]:
    """Parse coordinates from POINT string format."""
    if not point_str:
        return None, None

    # Extract coordinates from POINT (longitude latitude) format
    match = re.search(r"POINT \(([0-9.-]+) ([0-9.-]+)\)", point_str)
    if match:
        longitude = parse_float(match.group(1))
        latitude = parse_float(match.group(2))
        return latitude, longitude

    return None, None


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of castle records."""
    for _, row in batch_df.iterrows():
        try:
            # Parse coordinates
            latitude, longitude = parse_coordinates_from_point(row.get("SHAPE"))

            # Create stakeholder
            stakeholder_data = {
                "name": parse_names_and_identifiers(row.get("NAME")),
                "description": parse_content(row.get("BESCHREIBUNG")),
                "ownership": "private",
                "latitude": latitude,
                "longitude": longitude,
                "data_source_type": "Open Data",
                "data_source_name": "Austria data.gv.at",
            }

            # Filter out None values
            stakeholder_data = {
                k: v for k, v in stakeholder_data.items() if v is not None
            }

            if not stakeholder_data.get("name"):
                logging.warning(f"Skipping row with missing name: {row.to_dict()}")
                continue

            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={"name": stakeholder_data["name"]},
                **{k: v for k, v in stakeholder_data.items() if k != "name"},
            )

            session.flush()

            # Create organization type junction (GLAM - type_number 1)
            org_type, _ = get_or_create(
                session, OrganizationType, unique_key={"type_number": 1}
            )

            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "organization_type_id": org_type.id,
                },
            )

            # Create heritage topic junction (Architectural heritage - topic_number 4)
            heritage_topic, _ = get_or_create(
                session, CHTopic, unique_key={"topic_number": 4}
            )

            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "heritage_topic_id": heritage_topic.id,
                },
            )

        except Exception as e:
            logging.error(f"Error processing row {row.get('NAME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load castle data from CSV file."""
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

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")
