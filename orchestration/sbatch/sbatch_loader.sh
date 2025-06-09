#!/bin/bash

#SBATCH --job-name=loader
#SBATCH --partition=standard
#SBATCH --time=72:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/dh_pipeline/data/logs_sbatch/loader/run_%j.log

mkdir -p /home/lu72hip/DIGICHer/dh_pipeline/data/logs_sbatch/loader/

if [ $# -ne 2 ]; then
    echo "Usage: $0 <source> <query_id>"
    exit 1
fi

SOURCE=$1
QUERY_ID=$2



# DONT FORGET DATABASE :)



cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate

python src/elt/loading/run_loader.py --source "$SOURCE" --query_id "$QUERY_ID"