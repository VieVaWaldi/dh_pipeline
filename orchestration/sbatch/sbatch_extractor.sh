#!/bin/bash

#SBATCH --job-name=extractor
#SBATCH --partition=standard
#SBATCH --time=72:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/dh_pipeline/data/logs_sbatch/extractor/run_%j.log

mkdir -p /home/lu72hip/DIGICHer/dh_pipeline/data/logs_sbatch/extractor/

if [ $# -ne 2 ]; then
    echo "Usage: $0 <source> <query_id>"
    exit 1
fi

SOURCE=$1
QUERY_ID=$2

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate

python src/elt/extraction/run_extractor.py --source "$SOURCE" --query_id "$QUERY_ID"
# python src/analytics/jobs/raw_file_analysis.py --source_query_id cordis-query_id-0