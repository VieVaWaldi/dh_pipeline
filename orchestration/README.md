# Orchestration with Apache Airflow

Use `$ airflow standalone` for local development and testing.
Run Airflow webserver + scheduler on the login node in prod on the cluster > how?

## Setup

Ensure `export AIRFLOW_HOME=~/.airflow` is set up in .bashrc or in .zshrc

Also, set up the config in `~/.airflow/airflow.cfg` with:

- in line #7 `dags_folder = /path/from/root/.../dh_pipeline/orchestration`, to point to the correct DAG folder.
- in line #150 `load_examples = False`.

Then do this for the first initialization:
```
airflow db migrate # initializes the database
airflow users create --username admin --firstname Admin --lastname User \
    --role Admin --email admin@example.com
```

## Reset Airflow

```
# Kill airflow completely
pkill -f airflow

# Clear the database again (since examples are still there)
airflow db reset

# Restart airflow
airflow standalone
```

## Custom Installation

* This shouldn't need to be done as Airflow is part of the requirements.txt
* Only do this when you use a different python version for the project

```
AIRFLOW_VERSION=3.0.1;
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')";
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt";

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```