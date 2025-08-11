# Source Extraction and Loading

Go [Here]((../../sql/sources/README.md)) for Source Transformation.

All of these Sources have their very own considerations and therefore specific extraction strategy considerations.

* [Arxiv](arxiv/README.md)
* [Cordis](cordis/README.md)
* [Coreac](coreac/README.md)
* [OpenAire](openaire/README.md)

Data Source we are considering to add:

* Web of Science
* Scopus
* Europeana

## Strategy

1. Extract raw data from source given query in config, save data and checkpoint
2. Sanitize and transform raw data into SQL Alchemy models that mimic the source data model
3. Load SQL Alchemy models into the database

## Notes for self

* For SQLAlchemy, get_or_create only finds existing entities after flushing, so deduplicate lists with possible
  duplicates prior using a `have seen set`

