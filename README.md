# DIGICHer Pipeline

This is an EU funded project. The pipeline follows the EtLT pattern.

## Installation

The Project is developed in Python 3.11, setup like this:

Create a venv:

> python -m venv venv

Activate venv:

> source venv/bin/activate (Mac or Linux)

Installs the repo as an editable package and install all requirements:

> pip install -e .

Open `.env_`, add keys and rename it to `.env`. Comments after keys create errors.

## Structure

**Configuration**

- Under `/configs`, to configure queries and extration runs.
- Change prod or dev in `.env`.

**1. Orchestration and Clusters**

- ...

**2. Utils**

- Shared reusable code for single point of failure and logging.
- Under `/src/utils`
- Use `config_loader` to load the configuration files.
- Use `file_handling` and the `pathlib` for file operations.
- Use `web_requests` for all web requests including downloads.
- Use the various parsers to parse specific file types.

**3. Extractors**

- Under `/src/extractors`
- Extracts the data and loads it in the pile.
- Simply run the file from the top level directory.
- See more here [Link to Extractors Documentation](src/extractors/README.md)

**4. Analysis**

- ...

**5. Transformer**

- ...

**6. Data Fuser**

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


-----

# Extractors

For each extractor the interface `i_extractor` should be implemented.

It provides the following:

* Configures logging: Use `import logging` and call `logging.info()`.
* Member Variables for checkpoint and data paths.
    * `self.last_checkpoint`, reads and holds the last checkpoint value.
    * `self.checkpoint_path`, holds the path to the checkpoint.
    * `self.data_path`, holds the path to the data directory.
    * Each extracted record should have its own directory in `self.data_path`.
    * For further downloads create a directory called `attachments` in the record directory.

Furthermore, it enforces the following interface:

1. Create the end for the next checkpoint.
2. Extract data until next checkpoint.
3. Save the data.
4. Non contextually transform the data (This includes simple sanitization and attachment downloading).
5. Get the checkpoint.
6. Save the checkpoint.