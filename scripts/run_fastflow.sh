#!/bin/bash
#SBATCH --job-name=ff_mergesort
#SBATCH --output=/home/h.makhlouf/mergesort_project/data/ff_mergesort_%j.out
#SBATCH --error=/home/h.makhlouf/mergesort_project/data/ff_mergesort_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:02:00
#SBATCH --partition=normal

echo "=== Job Starting ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Date: $(date)"
echo "Node: $(hostname)"
echo "Initial directory: $(pwd)"

# Load modules
module load gnu12/12.2.0

# Change to the correct directory
echo "Changing to FastFlow directory..."
cd /home/h.makhlouf/mergesort_project/src/fastflow || { echo "Failed to cd to fastflow directory"; exit 1; }
echo "Current directory: $(pwd)"

# Check directory contents
echo "Directory contents:"
ls -la

# Create temp directory
echo "Creating temp directory..."
mkdir -p /home/h.makhlouf/mergesort_project/data/temp || { echo "Failed to create temp dir"; exit 1; }

# Build the project
echo "Building project..."
make clean && make || { echo "Build failed"; exit 1; }

# Check if executable exists
echo "Checking executable..."
ls -la mergesort_ff || { echo "Executable not found"; exit 1; }

# Run the program
echo "Running FastFlow MergeSort..."
./mergesort_ff /home/h.makhlouf/mergesort_project/data/input.bin /home/h.makhlouf/mergesort_project/data/output.bin 1000 4 || { echo "Execution failed with code $?"; exit 1; }

echo "=== Job Completed Successfully ==="
echo "End time: $(date)"
EOF

