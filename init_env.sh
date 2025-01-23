export SLURM_ACCOUNT=def-cva3
export SBATCH_ACCOUNT=$SLURM_ACCOUNT
export SALLOC_ACCOUNT=$SLURM_ACCOUNT

# Load the required module
module load python/3.12 scipy-stack

source ~/env_fl/bin/activate

