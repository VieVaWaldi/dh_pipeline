# Orchestration with Apache Airflow

## Connecting to Running Instance
* Password is here: cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
* Connect using tunnel: ssh -N -L 8080:localhost:8080 draco

## Local Development
Use `airflow standalone` for local development and testing.

## Production/Cluster Deployment
Use nohup for production deployment. This approach runs processes in the background and keeps them running even when you logout from the cluster.

**Start Airflow:**
```bash
nohup airflow api-server --port 8080 &> data/logs/airflow/airflow-api-server.log &
nohup airflow scheduler &> data/logs/airflow/airflow-scheduler.log &
```

**Managing nohup Services:**
```bash
# Check if services are running
ps aux | grep airflow

# Stop services
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Hard stop all airflow processes
pkill -9 -f airflow
```

## Errors :(

```bash
# Airflow doesnt find ur DAGs:
airflow dags reserialize

# WebApp running even though u used pkill?
lsof -i :8080
lsof -i :8793
kill [the processes in lsof]
```

## Orchestration Flow WIP

- In `run_pipeline.py`, runs every 30 days.
- Starts extractors and loaders as sbatch scripts with --wait and retries up to 3 times when an error occurred.
- Each loader waits for its extractor
- Transformation starts after all loaders are done
- Enrichment starts sequentially after transformation is done.

## Setup

Ensure `export AIRFLOW_HOME=~/.airflow` is set up in .bashrc or in .zshrc

Run `airflow db migrate` to initialize the database.

Set up the config in `~/.airflow/airflow.cfg` with:

- in line #7 `dags_folder = /path/from/root/.../dh_pipeline/orchestration`, to point to the correct DAG folder.
- in line #150 `load_examples = False`.

Finally run `airflow db reset` to ud-pate the config.

Test using `airflow dags list`, this should show the pipeline_dag (if not check `airflow standalone` first, sometimes it
takes time). Check errors with `airflow info ` & `airflow dags list-import-errors`.

## Reset Airflow

```
# Kill airflow completely
pkill -9 -f airflow

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
AIRFLOW_VERSION=3.0.1
PYTHON_VERSION="3.11";
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt";
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}";
```