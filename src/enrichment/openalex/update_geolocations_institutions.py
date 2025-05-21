import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from lib.loader import create_db_session
from loader import log_run_time
from lib.file_handling.file_utils import get_project_root_path, ensure_path_exists
from lib.sanitizers.sanitizer import (
    clean_geolocation,
)
from datamodels.digicher.entities import Institutions
from utils.config.config_loader import get_config
from utils.logger import setup_logging


class ModelUpdateMonitor:
    _stats = {}

    @classmethod
    def record(cls, model_name: str, is_updated: bool):
        if model_name not in cls._stats:
            cls._stats[model_name] = {"updated": 0}

        if is_updated:
            cls._stats[model_name]["updated"] += 1

    @classmethod
    def log_stats(cls):
        logging.info("=== Model Update Statistics ===")
        for model_name, stats in cls._stats.items():
            logging.info(f"{model_name:<30} Updated: {stats['updated']:>7}")


def should_update_geolocation(geo_info: str, distance: float, has_matching_name: bool):
    if geo_info is None or geo_info == "" or pd.isna(geo_info):
        return True
    return distance > 10000 and has_matching_name


config = get_config()
enrichment_name = "update_geolocation"
logging_path: Path = (
        get_project_root_path() / config["logging_path"] / "enrichment" / enrichment_name
)
ensure_path_exists(logging_path)
setup_logging(logging_path, enrichment_name)

SHOULD_UPDATE_COUNT = 0

df = pd.read_csv(
    # "cordis_openalex_geo_offset_18650.csv",
    # "openalex_geo_offset_21200.csv",
    # "openalex_geo_offset_31500.csv",

    # "cordis_2_openalex_geo_offset_800.csv",
    # "cordis_2_openalex_geo_offset_1350.csv",
    # "cordis_2_openalex_geo_offset_1600.csv",
    "output/cordis_2_openalex_geo_offset_5450.csv",
    
    sep=";",
)

BATCH_SIZE = 100
session_factory = create_db_session()

logging.info(f"Starting enrichment with {len(df)} data points")
mask = df.apply(
    lambda r: should_update_geolocation(
        r["institution_geolocation"], float(r["dist (m)"]), True
    ),
    axis=1,
)
cnt = len(df[mask])
logging.info(
    f"Will update {cnt} institutions which is {cnt/len(df)}% of the chosen data"
)

start_time = datetime.now()
with session_factory() as session:
    batch_cnt = 0
    for _, row in df.iterrows():

        # institution_id; institution_name; search_result_name; 
        # institution_geolocation; dist (m); source

        if not row["institution_name"] or pd.isna(row["institution_name"]):
            continue
        if not row["institution_geolocation"] or pd.isna(row["institution_geolocation"]):
            continue
        # if not should_update_geolocation(
        #     row["institution_geolocation"], float(row["dist (m)"]), bool(row["matching_name"])
        # ):
        #     continue
        SHOULD_UPDATE_COUNT += 1

        ### UPSERT ###
        unique_key = {"name": row["institution_name"]}
        instance = session.scalar(select(Institutions).filter_by(**unique_key))
        session.add(instance)
        new_geolocation = clean_geolocation(row["institution_geolocation"], swap_lat_lon=False)
        instance.address_geolocation = new_geolocation
        instance.updated_at = datetime.now()

        ModelUpdateMonitor.record(Institutions.__tablename__, is_updated=True)
        ##############

        batch_cnt += 1
        if batch_cnt % BATCH_SIZE == 0:
            session.commit()

    session.commit()  # Always a final commit
    session.close()
    ModelUpdateMonitor.log_stats()
    log_run_time(start_time)
