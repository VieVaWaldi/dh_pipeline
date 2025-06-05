# Digital Humanities Pipeline

A data pipeline, model and warehouse for the digital humanities.

The pipeline follows the ELT pattern. Source data gets **(1) Extracted**, sanitized and **(2) Loaded** into the database,
and finally deduplicated, **(3) Transformed** and enriched into a unified data model.

All sources can be used independently, meaning if you just want to get data from one of the available sources, it is as
simple as (optionally) configuring the queries, entering the api keys as needed and running the extractor.
All sources have data models in SQL-Alchemy, meaning they can be loaded into any database.

Configure queries for the sources and paths [Here](config/README.md).

Find more information about the specific sources [Here](src/sources/README.md).

## Installation

The Project is developed in Python 3.11, setup like this:

Open `.env_`, add keys as needed and rename file to `.env`.
Here you can also change between `prod` and `dev` mode

Create a venv:

> python -m venv venv

Activate venv:

> source venv/bin/activate (Mac or Linux)

> \venv\Scripts\activate.bat or \venv\Scripts\activate.ps1 (Windows and PowerShell) 

Installs the repo as an editable package and install all requirements:

> pip install -e .

## Dependencies

The following software is used:

- postgresql 17.?
- dbt v..?

## Database

[This document](README_DB.md) explains how you can control the PostgreSQL server locally and on the cluster.

## Orchestration

[This document](orchestration/README.md) explains how orchestration with Airflow is set up and works.

## Project Structure

**Configuration:**

- Use `/configs/configs_queries.json`, to configure queries for sources and extraction runs.
- Use `/configs/configs.json`, to configure working paths and database connections.
- Click [Here](config/README.md) to get more information.

**Extraction:**

- Run extractions with `/src/extracer/run_extractor.py`.
- This uses the sources extractors.
- See more here [Link to Extractor Documentation](src/elt/extraction/README.md)

**Loading:**

- Run loaders with `/src/loader/run_loader.py`.
- This uses the sources loaders.
- See more here [Link to Loader Documentation](src/elt/loading/README.md)

**Transformation:**

- Under `/src/sql`
- Uses dbt
- ...
- See more here ...

**Analysis:**

- Under `/src/analysis`
- To analyse the content and amount of raw files.
- Helps ... the unique key job thingy ...

**Lib:**

- Find all shared code and utilities here under `/src/lib`
- Libs for `extraction` and `loading`.
- Use `file_handling` and the `pathlib` for file operations.
- Use `requests` for all web requests including downloads.
- Use the various parsers in `sanitizers` for sanitization.

**Utils:**

- Utils for config, logging and errors under `/src/utils`.
- Use `config_loader` to load the configuration files.
- Use `load_dotenv()` to make the API key available.
- Start the ... logger ...

**Tests**

- The directory `tests` mirrors the `src` directory
- Run all tests like this: `python -m unittest discover tests`

## Notes

- Because of Airflow dependencies we have to use SQLAlchemy <2.0

## Development Guidelines

- Use type hints always, for function parameters, return values and all member variables.
- Use load_dotenv() load api keys etc. and store them in .env.
- Use Black Formatter for clean code `pip install black`.
- Use utils for reusable code like request, file handling, file parsing, data validation, sanitization or others.
- Use the logging module to log info's and errors.
- Use a defensice approach. When a request fails: Log error, don't save the data and try again.
