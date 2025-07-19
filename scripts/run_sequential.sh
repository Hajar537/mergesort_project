#!/bin/bash
#SBATCH --job-name=seq_mergesort
#SBATCH --output=~/mergesort_project/data/seq_mergesort_%j.out
#SBATCH --error=~/mergesort_project/data/seq_mergesort_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:02:00
#SBATCH --partition=normal

module load gnu12/12.2.0
cd ~/mergesort_project/src/sequential
echo "Job ID: $SLURM_JOB_ID starting on $(hostname) at $(date)"
mkdir -p ~/mergesort_project/data/temp || { echo "Failed to create temp dir"; exit 1; }
make || { echo "Make failed"; exit 1; }
echo "Running mergesort_seq"
srun --time=00:02:00 ./mergesort_seq ~/mergesort_project/data/input.bin ~/mergesort_project/data/output.bin 1000 || { echo "srun failed with code $?"; exit 1; }
echo "Job completed at $(date)"
