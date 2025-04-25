# Scientometric DH Pipeline

The pipeline follows the EtLT pattern. Source data gets **Extracted**, sanitized and **Loaded** into the database, and finally deduplicated and **Transformed** into a unified data model.

Our sources can be used independently, meaning if you just want to get data from one of the available sources its as simple as
(optionally) configuring the query, entering the api key if needed and running the extractor. 
All sources have data models in SQL-Alchemy, meaning they can be loaded into any database.

Find more information about the specific sources [here](src/sources/README.md).

Configure queries for the sources and paths [here](config/README.md).

## Installation

The Project is developed in Python 3.11, setup like this:

Create a venv:

> python -m venv venv

Activate venv:

> source venv/bin/activate (Mac or Linux)

Installs the repo as an editable package and install all requirements:

> pip install -e .

Open `.env_`, add keys and rename it to `.env`. Comments after keys create errors.

## Database 

This document explains how you can control the [PostgreSQL](sql/README_DB.md) server locally and on the cluster.

## Orchestration

## Project Structure

**0. Configuration**

- Under `/configs`, to configure queries and extration runs.
- Change prod or dev in `.env`.

**1. Orchestration**

- ...

**2. Common Utils**

- Shared reusable code for single point of failure and logging.
- Under `/src/utils`
- Use `config_loader` to load the configuration files.
- Use `file_handling` and the `pathlib` for file operations.
- Use `web_requests` for all web requests including downloads.
- Use the various parsers to parse specific file types.

**3. Extractors **

- Under `/src/extractors`
- Extracts the data and loads it in the pile.
- Simply run the file from the top level directory.
- See more here [Link to Extractors Documentation](src/extractors/README.md)

**3. Loading**

- ...

**4. Transformation **

- ...

**5. Analysis**

- ...

**Tests**

- The directory `tests` mirrors the `src` directory
- Run all tests like this: `python -m unittest discover tests`


## Developement Guidelines

- Use Black Formatter for clean code `pip install black`.
- Use utils for reusable code like request, file handling, file parsing, data validation, sanitization or others.
- Use type hints for function parameters, return values and all member variables.
- Use load_dotenv() load api keys etc. and store them in .env.
- Use the logging module to log info's and errors.
- When a request fails: Log error, dont save the data and abort.
