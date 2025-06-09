# Digital Humanities Pipeline

A data pipeline, data model and warehouse for the digital humanities.

The pipeline follows the ELT pattern. Source data gets **1. Extracted**, sanitized and **2. Loaded** into the database,
and finally deduplicated, **3. Transformed** and enriched into a unified data model.

All sources can be used independently, meaning if you just want to get data from one of the available sources, it is as
simple as (optionally) configuring the queries, entering the api keys as needed and running the extractor.
All sources have ORM models in SQL-Alchemy, meaning they can be loaded into any database.

Configure queries for the sources and paths [Here](config/README.md).

Find more information about the specific sources [Here](src/sources/README.md).

## Installation

The Project is developed in Python 3.11, setup like this:

Open `.env_`, add keys as needed and rename file to `.env`.
Here you can also change between `prod` and `dev` mode.

Create a venv:

> python -m venv venv

Activate venv:

> source venv/bin/activate (Mac or Linux)

> \venv\Scripts\activate.bat or \venv\Scripts\activate.ps1 (Windows and PowerShell)

Installs the repo as an editable package and install all requirements:

> pip install -e .

## Installing new packages

Run `pip install pip-tools`, then do:

1. Add new package to `requirements.in`
2. `pip-compile requirements.in` (generates locked requirements.txt)
3. `pip-sync requirements.txt` (installs new packages, removes unused ones)
4. `pip install -e .`

### Database

A `PostgreSQL DB 17.2` is being used for the project.
[This document](README_DB.md) explains how you can control the PostgreSQL server locally and on the cluster.

### Orchestration

[This document](orchestration/README.md) explains how orchestration with Airflow is set up and works.

## Project Structure

**Configuration:**

- Use `/configs/configs_queries.json`, to configure queries for sources and extraction runs.
- Use `/configs/configs.json`, to configure paths and database connections.
- Click [Here](config/README.md) to get more information.
- Raw data for each sources is saved as `{source_name}-query_id-{id}`.

**Extraction:**

- Run extractions with `/src/extraction/run_extractor.py`.
- This uses the sources extractors.
- See more here [Link to Extractor Documentation](src/elt/extraction/README.md)

**Loading:**

- Run loaders with `/src/loading/run_loader.py`.
- This uses the sources loaders.
- See more [Link to Loader Documentation](src/elt/loading/README.md)

**Sources:**

- Code for source specific extraction and loading is in `/src/sources`.
- See more here [Link to Source Documentation](src/sources/README.md)

**Transformation:**

- Under `/src/transformation`
- Uses dbt
- ... WIP ...
- See more here ...

**Analysis:**

- Under `/src/analysis`
- To analyse the content and amount of raw files.

**Lib:**

- Find all shared code and utilities here under `/src/lib`
- Use `database` to connect to the database.
- Use `file_handling` for file and dictionary operations.
- Use `requests` for all web requests including downloads.
- Use the various parsers in `sanitizers` for sanitization.

**Utils:**

- Utils for config, logging and errors under `/src/utils`.
- Use `get_config()` to load the configuration files and `get_query_config()` for the source parameters.
- Use `load_dotenv()` to make the API key available.
- Use the logger like this: `setup_logging('module','name')`
- Fail Fast: `log_and_exit(...)` is used whenever a non-clearable exception happens.

**Tests**

- The directory `tests` mirrors the `src` directory
- Run all tests like this: `python -m unittest discover tests`

## Notes

- Because of Airflow dependencies we have to use SQLAlchemy <2.0

## Development Guidelines

- Always use type hints.
- Use load_dotenv() load api keys etc. and store them in .env.
- Use Black Formatter for clean code `pip install black`.
- Use utils for reusable code like request, file handling, file parsing, data validation, sanitization or others.
- Use the logging module to log info's and errors.
- Use a defensive approach. When a request fails, fail fast: Log error, don't save the data and try again.
