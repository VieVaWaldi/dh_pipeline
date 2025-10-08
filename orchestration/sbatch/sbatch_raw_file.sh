#!/bin/bash

#SBATCH --job-name=analytics_raw_file
#SBATCH --partition=standard
#SBATCH --time=06:00:00

SOURCE=$1

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate

python src/analytics/jobs/raw_file_analysis.py --source_query_id "$SOURCE"