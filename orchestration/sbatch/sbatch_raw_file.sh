#!/bin/bash

#SBATCH --job-name=analytics_raw_file
#SBATCH --partition=standard
#SBATCH --time=06:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/dh_pipeline/data/logs_sbatch/analytics/raw_file_%j.log

SOURCE=$1

pg_ctl -D $PGDATA stop
sleep 10
pg_ctl -D $PGDATA start
sleep 3

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate

python src/analytics/jobs/raw_file_analysis.py --source_query_id "$SOURCE"