"""
Generate ROR schema documentation
Explores the actual data to understand structure and relationships
"""

import duckdb
from pathlib import Path

ROR_DB = Path("/vast/lu72hip/data/duckdb/sources/ror_raw.duckdb")
con = duckdb.connect(str(ROR_DB))

print("# ROR SCHEMA DOCUMENTATION\n")

print("## BASIC STATISTICS")

total = con.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
print(f"Total organizations: {total:,}")

print("\nStatus distribution:")
status_dist = con.execute(
    """
    SELECT status, COUNT(*) as count
    FROM organizations
    GROUP BY status
    ORDER BY count DESC
"""
).df()
print(status_dist.to_string(index=False))

print("\nOrganization types:")
types_dist = con.execute(
    """
    SELECT types as type, COUNT(*) as count -- unnest(types) throws error
    FROM organizations
    GROUP BY type
    ORDER BY count DESC
"""
).df()
print(types_dist.to_string(index=False))

print("\nTop 10 countries:")
countries = con.execute(
    """
    SELECT 
        locations[1].geonames_details.country_name as country,
        COUNT(*) as count
    FROM organizations
    WHERE locations IS NOT NULL AND len(locations) > 0
    GROUP BY country
    ORDER BY count DESC
    LIMIT 10
"""
).df()
print(countries.to_string(index=False))

print("## FIELD COMPLETENESS")

completeness = con.execute(
    """
    SELECT 
        COUNT(*) as total,
        COUNT(id) as has_id,
        COUNT(names) as has_names,
        COUNT(types) as has_types,
        COUNT(established) as has_established,
        COUNT(locations) as has_locations,
        COUNT(links) as has_links,
        COUNT(relationships) as has_relationships,
        COUNT(external_ids) as has_external_ids,
        COUNT(domains) as has_domains
    FROM organizations
"""
).df()

print("\nField population:")
for col in completeness.columns:
    if col != "total":
        value = completeness[col].iloc[0]
        pct = (value / total) * 100
        print(f"{col:25} {value:>7,} ({pct:>5.1f}%)")

print("## RELATIONSHIPS")

with_rels = con.execute(
    """
    SELECT COUNT(*) 
    FROM organizations 
    WHERE relationships IS NOT NULL AND len(relationships) > 0
"""
).fetchone()[0]
print(f"Organizations with relationships: {with_rels:,} ({with_rels/total*100:.1f}%)")

# print("\nRelationship types:")
# rel_types = con.execute("""
#     SELECT
#         relationships.type as relationship_type, -- unnest(relationships).type and relationships.type throw error
#         COUNT(*) as count
#     FROM organizations
#     WHERE relationships IS NOT NULL
#     GROUP BY relationship_type
#     ORDER BY count DESC
# """).df()
# print(rel_types.to_string(index=False))

print("\nExample organization with relationships:")
example = con.execute(
    """
    SELECT 
        id,
        names[1].value as name,
        relationships
    FROM organizations
    WHERE relationships IS NOT NULL AND len(relationships) > 0
    LIMIT 1
"""
).fetchone()
print(f"ID: {example[0]}")
print(f"Name: {example[1]}")
print(f"Relationships: {example[2]}")

# print("## EXTERNAL IDs (Crosswalk to other systems)")

# ext_id_types = con.execute("""
#     SELECT
#         unnest(external_ids).type as id_type, -- unnest(external_ids).type error
#         COUNT(*) as count
#     FROM organizations
#     WHERE external_ids IS NOT NULL
#     GROUP BY id_type
#     ORDER BY count DESC
# """).df()
# print(ext_id_types.to_string(index=False))

# print("## NAMES (Multiple names per organization)")

# name_types = con.execute("""
#     SELECT
#         unnest(unnest(names).types) as name_type, -- error
#         COUNT(*) as count
#     FROM organizations
#     WHERE names IS NOT NULL
#     GROUP BY name_type
#     ORDER BY count DESC
# """).df()
# print(name_types.to_string(index=False))

multi_names = con.execute(
    """
    SELECT 
        AVG(len(names)) as avg_names_per_org,
        MAX(len(names)) as max_names_per_org
    FROM organizations
    WHERE names IS NOT NULL
"""
).fetchone()
print(f"\nAverage names per org: {multi_names[0]:.1f}")
print(f"Max names per org: {multi_names[1]}")

print("## SAMPLE RECORDS")

print("\nExample 1: University with relationships")
uni = con.execute(
    """
    SELECT 
        id,
        names[1].value as name,
        types,
        locations[1].geonames_details.country_name as country,
        established,
        len(relationships) as num_relationships
    FROM organizations
    WHERE 'education' = ANY(types)
      AND relationships IS NOT NULL
      AND len(relationships) > 2
    LIMIT 1
"""
).df()
print(uni.to_string(index=False))

print("\nExample 2: Funder organization")
funder = con.execute(
    """
    SELECT 
        id,
        names[1].value as name,
        types,
        locations[1].geonames_details.country_name as country
    FROM organizations
    WHERE 'funder' = ANY(types)
    LIMIT 1
"""
).df()
print(funder.to_string(index=False))

print("\nExample 3: Organization with many external IDs")
ext_ids = con.execute(
    """
    SELECT 
        id,
        names[1].value as name,
        len(external_ids) as num_external_ids,
        external_ids
    FROM organizations
    WHERE external_ids IS NOT NULL
    ORDER BY len(external_ids) DESC
    LIMIT 1
"""
).fetchone()
print(f"Name: {ext_ids[1]}")
print(f"Number of external ID systems: {ext_ids[2]}")
print(f"External IDs: {ext_ids[3]}")

print("## DATA QUALITY")

duplicates = con.execute(
    """
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT id, COUNT(*) as cnt
        FROM organizations
        GROUP BY id
        HAVING COUNT(*) > 1
    )
"""
).fetchone()[0]
print(f"Duplicate IDs: {duplicates}")

no_names = con.execute(
    """
    SELECT COUNT(*) 
    FROM organizations 
    WHERE names IS NULL OR len(names) = 0
"""
).fetchone()[0]
print(f"Organizations without names: {no_names}")

no_types = con.execute(
    """
    SELECT COUNT(*) 
    FROM organizations 
    WHERE types IS NULL OR len(types) = 0
"""
).fetchone()[0]
print(f"Organizations without types: {no_types}")

print("Job Done!")

con.close()
