# DIGICHer Pipeline

This is an EU funded project.

The pipeline follows the EtLT pattern.

## Installation

The Project is developed in Python 3.11, setup like this:

Create a venv:

> python -m venv venv

Activate venv:

> source source venv/bin/activate (Mac or Linux)

Installs the repo as an editable package and install all requirements:

> pip install -e .

Get .env from me and place in root folder for api_keys.

## Structure

1. Orchestration

- ...

2. Utils

- under `/src/utils`
- Shared Reusable Code for single point of failure and logging

3. Extractors

- under `/src/extractors`
- Extracts the data and stores it in the pile
- See more here [Link to Extractors Documentation](src/extractors/README.md)

4. Transformer

- ...

5. Data Fuser

- ...

## Developement Guidelines

- Use Pylint (https://marketplace.visualstudio.com/items?itemName=ms-python.pylint)
- and Black Formatter (https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter) for clean code.
- Use type hints for function parameters and return values
- Use load_dotenv() load api keys etc. and store them in .env
- Use the logging module to log infos and errors
- When a request fails: Log error, dont save the data and abort
