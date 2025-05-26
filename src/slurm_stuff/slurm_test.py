#!/usr/bin/env python3
"""
SLURM Job Runner Test

Tests basic SLURM job submission, monitoring, and result collection from Python.
"""

import os
import sys
import time
import tempfile
import subprocess
import argparse
import re
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description="Test SLURM job submission from Python")
    parser.add_argument("--partition", default="power-owurtzel", help="SLURM partition")
    parser.add_argument("--cpus", default=4, type=int, help="CPUs per task")
    parser.add_argument("--mem", default="4GB", help="Memory allocation")
    parser.add_argument("--time", default="0:10:00", help="Time limit (HH:MM:SS)")
    parser.add_argument("--job-name", default="python_test", help="Job name")
    return parser.parse_args()

def create_test_job(args, output_dir):
    """Create a test job script that performs a simple calculation."""
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a simple test script
    script_path = os.path.join(output_dir, "test_job.sh")
    result_file = os.path.join(output_dir, "result.txt")
    
    with open(script_path, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(f"#SBATCH --job-name={args.job_name}\n")
        f.write(f"#SBATCH --output={output_dir}/slurm_%j.out\n")
        f.write(f"#SBATCH --error={output_dir}/slurm_%j.err\n")
        f.write(f"#SBATCH --partition={args.partition}\n")
        f.write(f"#SBATCH --time={args.time}\n")
        f.write(f"#SBATCH --mem={args.mem}\n")
        f.write(f"#SBATCH --cpus-per-task={args.cpus}\n")
        f.write("\n")
        
        # Add some commands that produce output
        f.write("echo 'SLURM job started'\n")
        f.write("hostname\n")
        f.write("date\n")
        f.write("sleep 20\n")  # Simulate some processing time
        f.write("echo 'Calculating prime numbers...'\n")
        
        # Create a calculation that produces a result file
        f.write("""
# Simple prime number finder
for ((i=2; i<=100; i++)); do
    is_prime=1
    for ((j=2; j<i; j++)); do
        if [ $((i % j)) -eq 0 ]; then
            is_prime=0
            break
        fi
    done
    if [ $is_prime -eq 1 ]; then
        echo $i
    fi
done > """ + result_file + "\n")
        
        f.write("echo 'Calculation complete'\n")
        f.write("echo 'Found '$(wc -l < " + result_file + ")" + "' prime numbers'\n")
        f.write("date\n")
        f.write("echo 'SLURM job completed'\n")
    
    # Make the script executable
    os.chmod(script_path, 0o755)
    return script_path, result_file

def submit_job(script_path):
    """Submit a job to SLURM and return the job ID."""
    try:
        result = subprocess.run(["sbatch", script_path], 
                               capture_output=True, text=True, check=True)
        
        # Extract job ID from output (format: "Submitted batch job 12345")
        match = re.search(r"Submitted batch job (\d+)", result.stdout)
        if match:
            job_id = match.group(1)
            print(f"Job submitted successfully with ID: {job_id}")
            return job_id
        else:
            print(f"Could not extract job ID from output: {result.stdout}")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error submitting job: {e.stderr}")
        return None

def check_job_status(job_id):
    """Check the status of a SLURM job."""
    try:
        result = subprocess.run(
            ["sacct", "-j", job_id, "--format=JobID,State,ExitCode", "--noheader", "--parsable2"],
            capture_output=True, text=True, check=True
        )
        
        # Process the result (may have multiple lines for job steps)
        lines = [line for line in result.stdout.strip().split('\n') if line]
        for line in lines:
            parts = line.split('|')
            if len(parts) >= 2:
                # Extract the main job ID (without .batch, etc.)
                line_job_id = parts[0].split('.')[0]
                if line_job_id == job_id:
                    state = parts[1]
                    return state
        
        return "UNKNOWN"
    except subprocess.CalledProcessError as e:
        print(f"Error checking job status: {e.stderr}")
        return "ERROR"

def wait_for_job_completion(job_id, poll_interval=10):
    """Wait for a SLURM job to complete."""
    print(f"Waiting for job {job_id} to complete...")
    
    while True:
        status = check_job_status(job_id)
        
        if status in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"):
            print(f"Job {job_id} finished with status: {status}")
            return status
        
        print(f"Job {job_id} status: {status}, waiting...")
        time.sleep(poll_interval)

def collect_results(job_id, output_dir, result_file):
    """Collect and display job results."""
    # Find the output file
    output_file = os.path.join(output_dir, f"slurm_{job_id}.out")
    error_file = os.path.join(output_dir, f"slurm_{job_id}.err")
    
    print("\n=== Job Output ===")
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            print(f.read())
    else:
        print(f"Output file not found: {output_file}")
    
    print("\n=== Job Errors ===")
    if os.path.exists(error_file) and os.path.getsize(error_file) > 0:
        with open(error_file, 'r') as f:
            print(f.read())
    else:
        print("No errors reported")
    
    print("\n=== Job Results ===")
    if os.path.exists(result_file):
        with open(result_file, 'r') as f:
            primes = f.read().strip().split('\n')
            print(f"Found {len(primes)} prime numbers between 1-100:")
            print(', '.join(primes[:10]) + "..." if len(primes) > 10 else ', '.join(primes))
    else:
        print(f"Result file not found: {result_file}")

def main():
    args = parse_args()
    
    # Create output directory
    output_dir = os.path.abspath("slurm_test_output")
    print(f"Using output directory: {output_dir}")
    
    # Create the job script
    script_path, result_file = create_test_job(args, output_dir)
    print(f"Created job script: {script_path}")
    
    # Submit the job
    job_id = submit_job(script_path)
    if not job_id:
        print("Failed to submit job")
        return 1
    
    # Wait for job completion
    final_status = wait_for_job_completion(job_id)
    
    # Collect and display results
    if final_status == "COMPLETED":
        collect_results(job_id, output_dir, result_file)
        return 0
    else:
        print(f"Job failed with status: {final_status}")
        return 1

if __name__ == "__main__":
    sys.exit(main())