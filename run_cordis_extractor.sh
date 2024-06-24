#!/bin/bash

# CHANGE FOR SBATCH

source venv/bin/activate
export PYTHONPATH=$PYTHONPATH:/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/src
python src/extractors/run_cordis_extractor.py
