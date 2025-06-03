#!/bin/bash

#SBATCH --job-name=validate_full_keys
#SBATCH --partition=standard
#SBATCH --time=02:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/DIGICHer_Pipeline/data/runs/analysis/validate_full_keys_%j.log

### Script

cd /home/lu72hip/DIGICHer/DIGICHer_Pipeline || exit
source venv/bin/activate

echo "Current user: $(whoami)"
echo "Current directory: $(pwd)"
echo "Python version: $(python --version)"

python src/analysis/jobs/validate_full_key_values.py