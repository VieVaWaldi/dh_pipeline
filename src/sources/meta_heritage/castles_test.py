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
    parse_geolocation,
    parse_web_resources,
)
from sources.meta_heritage.data_model import (
    Stakeholder,
    NutsCode,
    OrganizationType,
    CHTopic,
    JunctionStakeholderOrganizationType,
    JunctionStakeholderHeritageTopic,
)
from utils.config.config_loader import get_config, get_source_data_path
from utils.logger.logger import setup_logging


def extract_coordinates_from_point(
    shape_str: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Extract longitude and latitude from POINT geometry string."""
    if not shape_str or not shape_str.startswith("POINT"):
        return None, None

    try:
        # Extract coordinates from "POINT (longitude latitude)" format
        coords_part = shape_str.replace("POINT (", "").replace(")", "")
        lon_str, lat_str = coords_part.split()
        longitude = parse_float(lon_str)
        latitude = parse_float(lat_str)
        return longitude, latitude
    except (ValueError, AttributeError):
        logging.warning(f"Could not parse coordinates from: {shape_str}")
        return None, None


def create_stakeholder(session: Session, row: pd.Series) -> Stakeholder:
    """Create stakeholder entity from CSV row."""
    # Extract coordinates
    longitude, latitude = extract_coordinates_from_point(row.get("SHAPE"))

    # Create NUTS code for Vienna
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={"country_code": "AT", "level_1": "AT1", "level_2": "AT13"},
    )

    stakeholder_data = {
        "name": parse_names_and_identifiers(row.get("NAME")),
        "description": parse_content(row.get("BESCHREIBUNG")),
        "ownership": "public",
        "city": "Wien",
        "country": "Austria",
        "latitude": latitude,
        "longitude": longitude,
        "nuts_code_id": nuts_code.id,
        "data_source_type": "government_open_data",
        "data_source_name": "Austria data.gv.at",
    }

    # Filter out None values
    stakeholder_data = {k: v for k, v in stakeholder_data.items() if v is not None}

    stakeholder, _ = get_or_create(
        session,
        Stakeholder,
        unique_key={"name": stakeholder_data["name"]},
        **stakeholder_data,
    )

    return stakeholder


def create_organization_type_junction(session: Session, stakeholder: Stakeholder):
    """Create junction between stakeholder and GLAM organization type."""
    # Get GLAM organization type (type_number=1)
    org_type, _ = get_or_create(
        session, OrganizationType, unique_key={"type_number": 1}
    )

    # Create junction
    junction, _ = get_or_create(
        session,
        JunctionStakeholderOrganizationType,
        unique_key={
            "stakeholder_id": stakeholder.id,
            "organization_type_id": org_type.id,
        },
    )


def create_heritage_topic_junction(session: Session, stakeholder: Stakeholder):
    """Create junction between stakeholder and aristocratic heritage topic."""
    # Get aristocratic heritage topic (topic_number=1)
    ch_topic, _ = get_or_create(session, CHTopic, unique_key={"topic_number": 1})

    # Create junction
    junction, _ = get_or_create(
        session,
        JunctionStakeholderHeritageTopic,
        unique_key={"stakeholder_id": stakeholder.id, "heritage_topic_id": ch_topic.id},
    )


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of castle records."""
    for _, row in batch_df.iterrows():
        try:
            # Skip records without name
            if not row.get("NAME"):
                continue

            # Create stakeholder entity
            stakeholder = create_stakeholder(session, row)

            # Flush to ensure stakeholder has an ID
            session.flush()

            # Create junctions
            create_organization_type_junction(session, stakeholder)
            create_heritage_topic_junction(session, stakeholder)

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
