# Orchestration with Apache Airflow 

## Setup

Ensure `export AIRFLOW_HOME=~/airflow` is setup in .bashrc or in .zshrc 

## Custom Installation

* This shouldnt need to be done as Airflow is part of the requirements.txt
* Only do this when you use a different python version for the project

```
AIRFLOW_VERSION=3.0.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```