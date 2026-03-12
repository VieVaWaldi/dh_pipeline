"""
ROR Raw Ingestion - Load into DuckDB
"""

import logging
from datetime import datetime
from pathlib import Path

from lib.database.duck.create_connection import create_duck_connection
from lib.database.duck.utils import get_size_log
from lib.file_handling.file_utils import ensure_path_exists
from utils.config.config_loader import get_query_config
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time

setup_logging("loader", "ror_dump")

config = get_query_config()["ror_dump"]
ROR_SOURCE = Path(config["path_raw"])
ROR_DB = Path(config["path_duck"])

ensure_path_exists(ROR_DB)

logging.info(f"ROR INGESTION")
logging.info(f"Source: {ROR_SOURCE}")
logging.info(f"Target: {ROR_DB}")

start_time = datetime.now()

con = create_duck_connection(str(ROR_DB))
con.execute(
    f"""
    CREATE TABLE organizations AS
    SELECT *
    FROM read_json('{ROR_SOURCE}', format='auto')
"""
)

log_run_time(start_time)

""" VERIFY """
count = con.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
logging.info(f"Total organizations: {count:,}")

schema = con.execute("DESCRIBE organizations").df()
logging.info(f"Table schema:\n{schema.to_string(index=False)}")

sample = con.execute(
    "SELECT id, names[1].value as name, status, types FROM organizations LIMIT 3"
).df()
logging.info(f"Sample record (first organization):\n{sample.to_string(index=False)}")

logging.info(get_size_log(ROR_SOURCE, ROR_DB))

logging.info(f"Database location: {ROR_DB}")

con.close()
