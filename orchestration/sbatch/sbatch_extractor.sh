#!/bin/bash

#SBATCH --job-name=extractor
#SBATCH --partition=long
#SBATCH --time=144:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/dh_pipeline/data/logs/orchestration/extractor/run_%j.log

mkdir -p /home/lu72hip/DIGICHer/dh_pipeline/data/logs/orchestration/extractor/

if [ $# -ne 2 ]; then
    echo "Usage: $0 <source> <query_id>"
    exit 1
fi

SOURCE=$1
QUERY_ID=$2

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate

python src/elt/extraction/run_extractor.py --source "$SOURCE" --query_id "$QUERY_ID"
#python src/analysis/jobs/file_structure_analysis.py -q cordis_contenttypeISprojectANDSTAR