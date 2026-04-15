#!/bin/bash
#SBATCH --mem=16G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00
#SBATCH --mail-user=<isa45@sfu.ca>
#SBATCH --mail-type=ALL

cd ../code/daily/
module load python/3.12 scipy-stack
source ~/env_fl/bin/activate

START=${1:-0}
BATCH_SIZE=${2:-50}

python train.py "$START" "$BATCH_SIZE"
