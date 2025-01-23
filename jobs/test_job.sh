#!/bin/bash
#SBATCH --mem=32G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=0:0:20    
#SBATCH --mail-user=<isa45@sfu.ca>
#SBATCH --mail-type=ALL

cd ../code/
module load python/3.12 scipy-stack
source ~/env_fl/bin/activate

python myscript.py
