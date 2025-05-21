# Source Transformation

Transform source data models into our core data model.

We use dbt for the transformation, see more information [Here](README_DBT.md).

Go [Here](../src/sources/README.md) for Source Extraction & Loading.

Go [Here](README_DB.md) for the database documentation.

*General Process:*

1. Analysis and Description of Source
2. Deduplication of Source
3. Transformation from Source to Core (with checkpoints)
4. Deduplication of Core
5. Enrichment of Core

*Dependencies:*

- Source Data Model, matching Source Schema
- Core Data Model, our data schema

All source sql code is under `src/sql/sources`, each folder following the same pattern.

## Core Data Model

* enum source type for provenance, also including original id from sources
    * shows only original source type, after deduplication and merging might change a bit
* core models (ro, person, topic, link, publisher, journal) (later project, institution ...)
* core enrichment models (open alex) (more later)

## Transformation from Source to Core

### Checkpoint Mechanism

* Timestamp-based tracking using the `last_processed_timestamp` for each source table
* Stored in `core.import_checkpoint` with source_system and table_name as composite key
* Only need checkpoints for the "driving" tables from each source that contain the main entities
* Works universally for both new additions and updates to existing records

## Deduplication

1. Match on DOI
2. Match on normalized titles with similarity
3. Decide from here how to remove or combine duplicates

*No:* 
* Deduplication of Junctions, they are deleted on Cascade

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
