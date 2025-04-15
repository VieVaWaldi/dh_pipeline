#!/bin/bash

#SBATCH --job-name=data_loader
#SBATCH --partition=standard
#SBATCH --time=24:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/DIGICHer_Pipeline/data/runs/data_loader/dataloader_%j.log

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
pg_ctl -D $PGDATA stop # Stop it in case its running on the login node
pg_ctl -D $PGDATA start -l /vast/lu72hip/pgsql/logfile
sleep 5
echo "PostgreSQL status:"
pg_ctl status -D $PGDATA

### Script

cd /home/lu72hip/DIGICHer/DIGICHer_Pipeline || exit
source venv/bin/activate

echo "Current user: $(whoami)"
echo "Current directory: $(pwd)"
echo "Python version: $(python --version)"

python src/core/etl/dataloader/dataloader.py # --source cordis