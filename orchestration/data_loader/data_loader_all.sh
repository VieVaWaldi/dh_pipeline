#!/bin/bash

#SBATCH --job-name=data_loader
#SBATCH --partition=standard
#SBATCH --time=24:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/DIGICHer_Pipeline/data/runs/data_loader/data_loader_all_%j.log

### CleanUp

cleanup() {
    echo "Cleaning up..."
    pg_ctl stop -D "$PGDATA"
    exit
}
trap cleanup EXIT SIGINT SIGTERM

### Setup

echo "Running script ..."
echo "${PGDATA:?not set}"
echo "Stopping DB in case its running somewhere else"
pg_ctl -D $PGDATA stop
sleep 10

echo "Starting DB on this node"
pg_ctl -D $PGDATA start -l /vast/lu72hip/pgsql/logfile
sleep 10
echo "PostgreSQL status:"
pg_ctl status -D $PGDATA

### Scripts

cd /home/lu72hip/DIGICHer/DIGICHer_Pipeline || exit
source venv/bin/activate

echo "Current user: $(whoami)"
echo "Current directory: $(pwd)"
echo "Python version: $(python --version)"

python src/core/data_loader/run_data_loader.py --source cordis --run 0
sleep 300

# python src/core/data_loader/run_data_loader.py --source openaire --run 0
# sleep 300

# python src/core/data_loader/run_data_loader.py --source openaire --run 1
# sleep 300

python src/core/data_loader/run_data_loader.py --source coreac --run 0
sleep 300

python src/core/data_loader/run_data_loader.py --source arxiv --run 0
sleep 300