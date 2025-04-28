# Source Transformation

Go [Here](../src/sources/README.md) for Source Extraction & Loading.

Go [Here](README_DB.md) for the database documentation.

SQL Transformation to transform the source tables into our core data model.

## Core Data Model

* enum source type for provenance, also including original id from sources
   * shows only original source type, after deduplication and merging might change a bit 
* core models (ro, person, topic, link, publisher, journal) (later project, institution ...)
* core enrichment models (open alex) (more later)

## Checkpoint Mechanism

* Timestamp-based tracking using the `last_processed_timestamp` for each source table
* Stored in `core.import_checkpoint` with source_system and table_name as composite key
* Only need checkpoints for the "driving" tables from each source that contain the main entities
* Works universally for both new additions and updates to existing records

## Incremental Transformation Process

1. **Setup**
   * Create temporary batch tracking table for processing
   * Initialize with last checkpoint timestamp from previous run
   * Set desired batch size (default: 1000 records)

2. **Batch Processing Loop**
   * Process in manageable batches until all new/updated records are processed
   * Each batch is its own transaction for safe incremental processing

3. **For Each Batch**
   * **Main Entity Transformation**
     * Extract records from source updated since last checkpoint
     * Map to core schema fields
     * Insert with `ON CONFLICT` handling to update existing records
   
   * **Related Entity Transformation**
     * For each related entity type (people, topics, links, etc.):
       * Extract related records for current batch
       * Insert new entities with `ON CONFLICT DO NOTHING`
       * Create junction records linking to main entities
   
   * **Checkpoint Update**
     * Update checkpoint with timestamp of most recent record processed
     * Commit transaction before moving to next batch

## Standard Pattern for Entity Resolution

For each related entity (people, topics, etc.):

```sql
WITH entity_entries AS (
    -- Extract related entities for current batch
),
inserted_entities AS (
    -- Insert unique entities
    INSERT INTO core.entity_table (...)
    SELECT DISTINCT ... FROM entity_entries
    ON CONFLICT (...) DO NOTHING
    RETURNING id, identifying_field
),
all_entities AS (
    -- Combine newly inserted + existing entities
    SELECT id, identifying_field FROM inserted_entities
    UNION ALL
    SELECT e.id, e.identifying_field FROM entity_entries ee
    JOIN core.entity_table e ON e.identifying_field = ee.identifying_field
    WHERE NOT EXISTS (
        SELECT 1 FROM inserted_entities ie 
        WHERE ie.identifying_field = ee.identifying_field
    )
)
-- Create junction records
INSERT INTO core.j_main_entity (...)
SELECT ... FROM entity_entries ...
JOIN all_entities ... ON ...
ON CONFLICT (...) DO NOTHING;
```

## Deduplication

1. Match on DOI
2. Match on normalized titles with similarity
3. Decide from here how to remove or combine duplicates

## Enrichment

* Open Alex
   * Publication Information like refs etc
   * Adding missing geolocations to institutions
* Crossref
   * Information for each paper with a DOI 
* Mapbox
   * Adding missing geolocations to institutions 
* LLM Extraction - CURRENTLY ONLY CORDIS
   * get title, pub_date if not there
   * get language code
   * research output type given index for n characters
   * find funding number, maybe normal search better
