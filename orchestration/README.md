# Orchestration with Apache Airflow

# Connecting to Running Instance
* Password is here: cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
* IMPORTANT: idk why but it keeps adding passwords to the file. You must delete all but the first one for it to work.
* Connect using tunnel: ssh -N -L 8080:localhost:8080 draco

## Local Development
Use `airflow standalone` for local development and testing.

## Production/Cluster Deployment
We use **supervisor** for production deployment on the cluster. Supervisor manages Airflow processes automatically, provides auto-restart on crashes, proper logging, and reliable process management.

**But Supervisor has to be restarted when the login node restarts.**

### Supervisor Configuration
Config file location: `~/.config/supervisor/supervisord.conf`

### Starting Airflow (Production)
```bash
# Start supervisor daemon (manages airflow processes)
supervisord -c ~/.config/supervisor/supervisord.conf
```

### Managing Supervisor Services
```bash
supervisorctl -c ~/.config/supervisor/supervisord.conf status
supervisorctl -c ~/.config/supervisor/supervisord.conf start airflow-scheduler
supervisorctl -c ~/.config/supervisor/supervisord.conf restart all
supervisorctl -c ~/.config/supervisor/supervisord.conf stop all
```

## Errors & Troubleshooting

### Common Issues
```bash
# Airflow doesn't find your DAGs
airflow dags reserialize

# Supervisor connection refused
# Clean up old socket files and restart:
pkill -f supervisord
rm -f ~/.config/supervisor/supervisor.sock
rm -f ~/.config/supervisor/supervisord.pid
supervisord -c ~/.config/supervisor/supervisord.conf

# Check DAG syntax errors
airflow dags list-import-errors

# General airflow info
airflow info
```

### Manual Process Management
```bash
ps aux | grep airflow
pkill -f supervisord
pkill -9 -f airflow
```

### Port Issues
```bash
# Check what's using port 8080
lsof -i :8080
kill [process_id]
```

## Complete Airflow Reset

```bash
# Stop all services
supervisorctl -c ~/.config/supervisor/supervisord.conf stop all

# Kill any remaining processes
pkill -9 -f airflow

# Reset database (WARNING: deletes all DAGs, runs, users)
airflow db reset --yes
airflow dags reserialize

# Clear logs
rm -rf ~/.airflow/logs/*
rm -rf /home/lu72hip/DIGICHer/dh_pipeline/data/logs/airflow/*

# Restart services
supervisord -c ~/.config/supervisor/supervisord.conf
```

## Orchestration Flow WIP

- In `run_pipeline.py`, runs every 10 days.
- Starts extractors and loaders as sbatch scripts with --wait and retries up to 10 times after 1 hour, when an error occurred.
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