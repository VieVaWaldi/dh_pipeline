import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from core.etl.dataloader.create_db_session import create_db_session
from core.etl.dataloader.dataloader import log_run_time
from core.file_handling.file_handling import get_root_path, ensure_path_exists
from core.sanitizers.sanitizer import (
    clean_geolocation,
)
from datamodels.digicher.entities import Institutions
from utils.config.config_loader import get_config
from utils.logger.logger import setup_logging


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
    get_root_path() / config["logging_path"] / "enrichment" / enrichment_name
)
ensure_path_exists(logging_path)
setup_logging(logging_path, enrichment_name)

SHOULD_UPDATE_COUNT = 0

df = pd.read_csv(
    "openalex_geo_offset_18650.csv",
    # "openalex_geo_offset_21200.csv",
    # "openalex_geo_offset_31500.csv",
    sep=";",
)

BATCH_SIZE = 100
session_factory = create_db_session()

logging.info(f"Starting enrichment with {len(df)} data points")
mask = df.apply(
    lambda r: should_update_geolocation(
        r["i.geo"], float(r["dist (m)"]), bool(r["matching_name"])
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

        if not row["i.name"] or pd.isna(row["i.name"]):
            continue
        if not should_update_geolocation(
            row["i.geo"], float(row["dist (m)"]), bool(row["matching_name"])
        ):
            continue
        SHOULD_UPDATE_COUNT += 1

        ### UPSERT ###
        unique_key = {"name": row["i.name"]}
        instance = session.scalar(select(Institutions).filter_by(**unique_key))
        session.add(instance)
        new_geolocation = clean_geolocation(row["oa.geo"], swap_lat_lon=False)
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
