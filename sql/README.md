# Source Transformation

Go [Here](../src/sources/README.md) for Source Extraction & Loading.

Go [Here](README_DB.md) for the database documentation.

SQL Transformation to transform the source tables into our core data model.

## Core Data Model

* enum source type for provenance, also including original id from sources
   * shows only original source type, after deduplication and merging might change a bit 
* core models (ro, person, topic, link, publisher, journal) (later project, institution ...)
* core enrichment models (open alex) (more later)

## Strategy

First model transformation from source to core.

1. Run transformation per source
   1. Process main entry from current batch
      1. Extract and insert relevant data ON CONFLICT DO NOTHING
   2. For each related entity 
      1. extract relevant data
      2. insert new entities with ON CONFLICT DO NOTHING
      3. Create UNION of new and existing entities
      4. Create junction records
   3. Update CP and move to next batch
2. Deduplicate core tables
3. Run self-referential references for core.researchoutput
4. Core Indexing - All > Then Full Text 
5. Run Enrichment

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
   * get title if not there
   * get language code
   * ro type given index for first page
   * find funding number, maybe normal search better

## Checkpoint

* We only need to save checkpoints for the main tables as they drive the realtions
* simple id and updated_at timestamp tracking which works because of serial id