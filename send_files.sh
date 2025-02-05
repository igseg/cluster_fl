#!/bin/bash

tot_elem=1826
jumps=83
last_jump=$((jumps-1))
files=(/media/ignacio/TOSHIBA\ EXT/data/Tardis/*)

# start=0
start_file="start_file.txt"
start=$(cat "$start_file")

for jump in $(seq "$start" "$last_jump")
do
  start_read=$((jump*22))  # Update start before entering the second loop
  echo "$((jump))"
  echo "$jump" > "$start_file"
  read_files=("${files[@]:start_read:22}")  # Corrected slicing syntax
  # echo "${read_files[@]}"  # Print selected files
  scp -r "${read_files[@]}" igseta@cedar.computecanada.ca:scratch/full_tardis_data/
  echo "============================================================"
  # for num in {0..21}
  # do
  #   # echo ${files[num]}  # Uncomment if 'files' is defined
  #   echo "$((start+num))"
  # done
done
