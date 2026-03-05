"""
ROR EDA - Quick exploration
"""
import duckdb
import json
from pathlib import Path

ROR_FILE = Path("/vast/lu72hip/data/pile/ror_2026_02_24_dump/v2.3-2026-02-24-ror-data.json")
con = duckdb.connect()

print("# ROR EXPLORATION")

# Schema
print("\nSCHEMA:")
schema = con.execute(f"""
    DESCRIBE SELECT * FROM read_json('{ROR_FILE}', format='auto') LIMIT 1
""").df()
print(schema.to_string(index=False))

# Count
count = con.execute(f"SELECT COUNT(*) FROM read_json('{ROR_FILE}')").fetchone()[0]
print(f"\nTOTAL RECORDS: {count:,}")

# Sample record
print("\nFIRST RECORD (JSON):")
record = con.execute(f"SELECT * FROM read_json('{ROR_FILE}') LIMIT 1").df().iloc[0].to_dict()
print(json.dumps(record, indent=2, default=str))

print("\n" + "=" * 80)
print("DONE!")