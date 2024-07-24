# DIGICHer Pipeline

This is an EU funded project. The pipeline follows the EtLT pattern.

## Installation

The Project is developed in Python 3.11, setup like this:

Create a venv:

> python -m venv venv

Activate venv:

> source source venv/bin/activate (Mac or Linux)

Installs the repo as an editable package and install all requirements:

> pip install -e .

Open `.env_` and follow instructions.

## Structure

**Configuration**

- Under `/configs`, to configure queries and extration runs.
- Change prod or dev in `.env`.

**1. Orchestration**

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

**4. Transformer**

- ...

**5. Data Fuser**

- ...

## Developement Guidelines

- Use Black Formatter for clean code `pip install black`.
- Use utils for reusable code like request, file handling, file parsing, data validation, sanitization or others.
- Use type hints for function parameters, return values and all member variables.
- Use load_dotenv() load api keys etc. and store them in .env.
- Use the logging module to log info's and errors.
- When a request fails: Log error, dont save the data and abort.
