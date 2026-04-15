#!/bin/bash
# Submit train_daily.sh in batches covering [0, END) with step BATCH_SIZE.
# Usage: ./submit_train_daily.sh [END] [BATCH_SIZE]

END=${1:-1800}
BATCH_SIZE=${2:-50}

for (( start=0; start<END; start+=BATCH_SIZE )); do
    sbatch train_daily.sh "$start" "$BATCH_SIZE"
done
