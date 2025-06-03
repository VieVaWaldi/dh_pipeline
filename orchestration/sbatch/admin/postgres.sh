#!/bin/bash

#SBATCH --job-name=transformation
#SBATCH --partition=standard
#SBATCH --time=4:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/DIGICHer_Pipeline/data/runs/transformation/postgres_%j.log

### CleanUp

cleanup() {
   echo "Cleaning up..."
   pg_ctl stop -D "$PGDATA"
   exit
}
trap cleanup EXIT SIGINT SIGTERM

### Setup

echo "${PGDATA:?not set}"
pg_ctl -D $PGDATA stop # Stop it in case its running on the login node
pg_ctl -D $PGDATA start -l /vast/lu72hip/pgsql/logfile
sleep 5
echo "PostgreSQL status:"
pg_ctl status -D $PGDATA

# Print connection information
echo "====================== CONNECTION INFO ======================"
echo "Hostname: $(hostname)"
echo "Use this for your tunnel:"
echo "ssh -N -L 5433:localhost:5433 -J lu72hip@login2.draco.uni-jena.de lu72hip@$(hostname)"
echo "=========================================================="

echo "Server will run for 4 hours or until job is cancelled"
sleep 4h