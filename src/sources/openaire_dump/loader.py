"""
OpenAIRE Dump Ingestion - Load into DuckDB

Loads 4 entities in order:
  1. organization       (~448K rows)
  2. organization_pids  (derived via UNNEST from organization.pids)
  3. project            (~3.7M rows)
  4. publication        (~165M rows, from publication/ directory only)
  5. relation           (~2B raw -> filtered by relType + publication ID semi-join)

Target: single openaire.duckdb file.
"""

import logging
from datetime import datetime
from pathlib import Path

from lib.database.duck.create_connection import create_duck_connection
from lib.file_handling.file_utils import ensure_path_exists
from utils.config.config_loader import get_query_config
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time

setup_logging("loader", "openaire_dump")

config = get_query_config()["openaire_dump"]
SOURCE = Path(config["path_raw"])
DB = Path(config["path_duck"])

ensure_path_exists(DB)

logging.info("OPENAIRE DUMP INGESTION")
logging.info(f"Source: {SOURCE}")
logging.info(f"Target: {DB}")

con = create_duck_connection(str(DB))

# Override defaults from create_duck_connection — this job needs more
con.execute("SET memory_limit='160GB'")
con.execute("SET threads=32")

total_start = datetime.now()


# -------------------------------------------------------------------------
# 1. ORGANIZATION
# -------------------------------------------------------------------------
logging.info("--- Loading organization ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE organization AS
    SELECT * FROM read_json('{SOURCE}/organization/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM organization").fetchone()[0]
logging.info(f"organization rows: {count:,}")


# -------------------------------------------------------------------------
# 2. ORGANIZATION_PIDS  (derived — ROR merge key for step 4 of pipeline)
# -------------------------------------------------------------------------
logging.info("--- Loading organization_pids (unnested from organization.pids) ---")
t = datetime.now()

con.execute("""
    CREATE TABLE organization_pids AS
    SELECT id AS org_id, p.scheme, p.value
    FROM organization, UNNEST(pids) AS t(p)
    WHERE pids IS NOT NULL
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM organization_pids").fetchone()[0]
logging.info(f"organization_pids rows: {count:,}")

ror_count = con.execute("SELECT COUNT(*) FROM organization_pids WHERE scheme = 'ROR'").fetchone()[0]
logging.info(f"  of which ROR: {ror_count:,}")

# Note: ROR values are full URLs e.g. https://ror.org/00mesrk97
# Strip prefix when merging with ROR data in the sanitization step.


# -------------------------------------------------------------------------
# 3. PROJECT
# -------------------------------------------------------------------------
logging.info("--- Loading project ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE project AS
    SELECT * FROM read_json('{SOURCE}/project/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM project").fetchone()[0]
logging.info(f"project rows: {count:,}")


# -------------------------------------------------------------------------
# 4. PUBLICATION
# -------------------------------------------------------------------------
logging.info("--- Loading publication ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE publication AS
    SELECT * FROM read_json('{SOURCE}/publication/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM publication").fetchone()[0]
logging.info(f"publication rows: {count:,}")

# publicationDate has bad values (e.g. 0001-12-30) — loaded as-is, cleaned in sanitization step.


# -------------------------------------------------------------------------
# 5. RELATION  (filtered — must run after publication is loaded)
# -------------------------------------------------------------------------
# Filter logic:
#   a) relType.type must be one of: affiliation, outcome, citation, participation
#      (excludes provision and similarity which dominate raw volume)
#   b) Where either side is a 'product', that product must exist in publication.
#      We cannot filter to publication-type products during the scan itself
#      because relation rows only carry sourceType='product', not the product subtype.
#      So we semi-join against the already-loaded publication table.
#   c) participation (organization <-> project) has no product side — passes through.
#
logging.info("--- Loading relation (filtered) ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE relation AS
    SELECT r.*
    FROM read_json('{SOURCE}/relation/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true) r
    WHERE r.relType.type IN ('affiliation', 'outcome', 'citation', 'participation')
      AND (
          (r.sourceType = 'product' AND r.source IN (SELECT id FROM publication))
          OR (r.targetType = 'product' AND r.target IN (SELECT id FROM publication))
          OR (r.sourceType != 'product' AND r.targetType != 'product')
      )
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
logging.info(f"relation rows (filtered): {count:,}")

type_dist = con.execute("""
    SELECT relType.type, COUNT(*) as cnt
    FROM relation
    GROUP BY relType.type ORDER BY cnt DESC
""").fetchall()
logging.info("relation type distribution:")
for row in type_dist:
    logging.info(f"  {row[0]}: {row[1]:,}")


# -------------------------------------------------------------------------
# VERIFY
# -------------------------------------------------------------------------
logging.info("--- Final verification ---")

for table in ["organization", "organization_pids", "project", "publication", "relation"]:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logging.info(f"  {table}: {count:,} rows")

schema_pub = con.execute("DESCRIBE publication").df()
logging.info(f"publication schema:\n{schema_pub.to_string(index=False)}")

sample_pub = con.execute("""
    SELECT id, mainTitle, publicationDate, publisher, language.code
    FROM publication LIMIT 3
""").df()
logging.info(f"publication sample:\n{sample_pub.to_string(index=False)}")

sample_rel = con.execute("""
    SELECT source, sourceType, target, targetType, relType.type, relType.name
    FROM relation LIMIT 5
""").df()
logging.info(f"relation sample:\n{sample_rel.to_string(index=False)}")

db_size_mb = DB.stat().st_size / (1024 ** 2)
logging.info(f"DuckDB file size: {db_size_mb:,.1f} MB")

log_run_time(total_start)
logging.info(f"Database location: {DB}")

con.close()
