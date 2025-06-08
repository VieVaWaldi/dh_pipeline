# Orchestration with Apache Airflow

Use `airflow standalone` for local development and testing.

Use the following for production:

```
# Use & to run them in the background
# Starts the scheduler (monitors DAGs, triggers tasks)
airflow scheduler &

# Starts the web UI (default port 8080)
airflow api-server &

# Processes DAG files (optional, can run in scheduler)
airflow dag-processor 
```

## Orchestration Flow WIP

- In `run_pipeline.py`.
- Starts extractors and loaders as sbatch scripts with --wait and retries when an error occurred.
- Each loader waits for its extractor
- Transformation starts after all loaders are done
- Enrichment starts sequentially after transformation is done.

## Setup

Ensure `export AIRFLOW_HOME=~/.airflow` is set up in .bashrc or in .zshrc

Run `airflow db init` to initialize the database.

Set up the config in `~/.airflow/airflow.cfg` with:

- in line #7 `dags_folder = /path/from/root/.../dh_pipeline/orchestration`, to point to the correct DAG folder.
- in line #150 `load_examples = False`.

Finally run `airflow db migrate` to udpate the congfig.

## Reset Airflow

```
# Kill airflow completely
pkill -f airflow

# Clear the database
airflow db reset

# Restart airflow
airflow standalone
```

## Custom Installation

* This shouldn't need to be done as Airflow is part of the requirements.txt
* Only do this when you use a different python version for the project
* Because of a bug in the new Airflow version we are using 2.11

```
AIRFLOW_VERSION=2.11.0;
PYTHON_VERSION="3.11";
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt";
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}";
```