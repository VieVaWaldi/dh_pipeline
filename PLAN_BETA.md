# Core_v3

Core_v4 will be the harmonization between OpenAlex, OpenAire and ROR.
Core_v3 is OpenAire++, including ROR and enrichment.
Later we will be able to reuse ELT procedures we wrote for OpenAire.
But for now we want the surgical minimal subset of data from OpenAire,
which enables HeritageMonitor to give a real picture of CH research.

## High Level Plan

1. Pick all relevant Entities and Columns From OpenAire
2. Ingest them into duckdb
3. EDA -> Jupyter
- jupyter notebook --no-browser --port=8888
4. Deduplicate and clean OpenAire properly (no idea why they havent done that themselves)
5. Merge OpenAire institutions with ROR
6. Merge with Cordis for institution level funding information 
7. Enrich with OpenAlex TFIDF topic classification
8. Classify first Projects, then Works as CH
9. Deploy (We already have OpenAire in our data so even that step should be simpler)

## DAG 