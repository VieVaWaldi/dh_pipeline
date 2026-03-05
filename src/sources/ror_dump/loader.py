"""
ROR Raw Ingestion - Load into DuckDB
Super simple: Let DuckDB auto-create the table
"""

import duckdb
from pathlib import Path
from datetime import datetime
from lib.file_handling.file_utils import ensure_path_exists

""" PATHS """
ROR_SOURCE = Path("/vast/lu72hip/data/pile/ror_2026_02_24_dump/v2.3-2026-02-24-ror-data.json")
ROR_DB = Path("/vast/lu72hip/data/duckdb/sources/ror_raw.duckdb")

ensure_path_exists(ROR_DB)

print("ROR INGESTION")
print(f"Source: {ROR_SOURCE}")
print(f"Target: {ROR_DB}\n")


""" CONNECT & CONFIGURE """
con = duckdb.connect(str(ROR_DB))

# Memory settings (conservative for login node)
con.execute("SET memory_limit='16GB'")
con.execute("SET threads=8")
con.execute("SET enable_progress_bar=true")

print("Connected to DuckDB\n")

start_time = datetime.now()

# The magic one-liner!
con.execute(f"""
    CREATE TABLE organizations AS 
    SELECT * 
    FROM read_json('{ROR_SOURCE}', format='auto')
""")

elapsed = (datetime.now() - start_time).total_seconds()

print(f"✓ Table created in {elapsed:.1f} seconds\n")

""" VERIFY """
print("=" * 80)
print("VERIFICATION")
print("=" * 80)

# Count
count = con.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
print(f"Total organizations: {count:,}")

# Schema
print("\nTable schema:")
schema = con.execute("DESCRIBE organizations").df()
print(schema.to_string(index=False))

# Sample
print("\nSample record (first organization):")
sample = con.execute("SELECT id, names[1].value as name, status, types FROM organizations LIMIT 3").df()
print(sample.to_string(index=False))

# Database size
print("\nDatabase file size:")
db_size_mb = ROR_DB.stat().st_size / (1024 ** 2)
print(f"{db_size_mb:.1f} MB")

# Compression ratio
source_size_mb = ROR_SOURCE.stat().st_size / (1024 ** 2)
compression_ratio = source_size_mb / db_size_mb
print(f"\nCompression: {source_size_mb:.1f} MB → {db_size_mb:.1f} MB ({compression_ratio:.2f}x)")

print("\n" + "=" * 80)
print("✓ DONE!")
print("=" * 80)
print(f"\nDatabase location: {ROR_DB}")
print("You can now query it with: duckdb {ROR_DB}")

con.close()