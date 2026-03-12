#!/bin/bash
#SBATCH --job-name=openaire_ingest
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --time=18:00:00
#SBATCH --partition=fat
#SBATCH --output=/vast/lu72hip/logs/openaire_ingest_%j.log
#SBATCH --mail-user=walter.ehrenberger@uni-jena.de
#SBATCH --mail-type=ALL

mkdir -p /vast/lu72hip/logs

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate
export PYTHONPATH=/home/lu72hip/DIGICHer/dh_pipeline/src

python src/sources/openaire_dump/loader.py
