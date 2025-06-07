#!/bin/bash

#SBATCH --job-name=loader
#SBATCH --partition=long
#SBATCH --time=144:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/dh_pipeline/data/logs/orchestration/loader/run_%j.log

mkdir -p /home/lu72hip/DIGICHer/dh_pipeline/data/logs/orchestration/loader/

if [ $# -ne 2 ]; then
    echo "Usage: $0 <source> <query_id>"
    exit 1
fi

SOURCE=$1
QUERY_ID=$2

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate

python src/elt/loading/run_loader.py --source "$SOURCE" --query_id "$QUERY_ID"