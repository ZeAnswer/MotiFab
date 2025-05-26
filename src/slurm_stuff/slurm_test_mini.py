#!/usr/bin/env python3
"""
Minimalist SLURM Job Runner

Runs SLURM jobs with minimal file usage.
"""

import os
import sys
import time
import subprocess
import argparse
import re
import tempfile
import threading

def parse_args():
    parser = argparse.ArgumentParser(description="Run SLURM jobs with minimal file usage")
    parser.add_argument("--partition", default="power-owurtzel", help="SLURM partition")
    parser.add_argument("--cpus", default=4, type=int, help="CPUs per task")
    parser.add_argument("--mem", default="4GB", help="Memory allocation")
    parser.add_argument("--time", default="0:10:00", help="Time limit (HH:MM:SS)")
    parser.add_argument("--job-name", default="python_test", help="Job name")
    return parser.parse_args()

def check_job_status(job_id):
    """Check the status of a SLURM job."""
    try:
        result = subprocess.run(
            ["sacct", "-j", job_id, "--format=JobID,State", "--noheader", "--parsable2"],
            capture_output=True, text=True, check=True
        )
        
        lines = [line for line in result.stdout.strip().split('\n') if line]
        for line in lines:
            parts = line.split('|')
            if len(parts) >= 2 and parts[0].split('.')[0] == job_id:
                return parts[1]
        
        # Query squeue for pending/running jobs that might not show in sacct yet
        squeue_result = subprocess.run(
            ["squeue", "-j", job_id, "--noheader", "--format=%T"],
            capture_output=True, text=True
        )
        if squeue_result.returncode == 0 and squeue_result.stdout.strip():
            return squeue_result.stdout.strip()
            
        return "UNKNOWN"
    except Exception as e:
        print(f"Error checking job status: {e}")
        return "ERROR"

def wait_for_job_completion(job_id, poll_interval=5):
    """Wait for a SLURM job to complete."""
    print(f"Waiting for job {job_id} to complete...")
    
    while True:
        status = check_job_status(job_id)
        
        if status in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"):
            print(f"Job {job_id} finished with status: {status}")
            return status
        
        print(f"Job {job_id} status: {status}, waiting...")
        time.sleep(poll_interval)

def run_slurm_job_with_output(command, args):
    """Run a SLURM job and capture its output using a temporary file."""
    # Create a temp file for the output
    with tempfile.NamedTemporaryFile(delete=False, suffix='.out') as out_file:
        output_path = out_file.name
    
    # Create a temp file for the error
    with tempfile.NamedTemporaryFile(delete=False, suffix='.err') as err_file:
        error_path = err_file.name
    
    # Submit the job with output directed to our temp files
    cmd = [
        "sbatch",
        f"--job-name={args.job_name}",
        f"--partition={args.partition}",
        f"--time={args.time}",
        f"--mem={args.mem}",
        f"--cpus-per-task={args.cpus}",
        f"--output={output_path}",
        f"--error={error_path}",
        "--wrap", command
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        match = re.search(r"Submitted batch job (\d+)", result.stdout)
        if not match:
            print(f"Failed to extract job ID from: {result.stdout}")
            return None, "", "", "FAILED"
            
        job_id = match.group(1)
        print(f"Job submitted with ID: {job_id}")
        
        # Wait for job to complete
        final_status = wait_for_job_completion(job_id)
        
        # Get output and errors from temp files
        output = ""
        errors = ""
        
        try:
            with open(output_path, 'r') as f:
                output = f.read()
        except Exception as e:
            print(f"Error reading output file: {e}")
        
        try:
            with open(error_path, 'r') as f:
                errors = f.read()
        except Exception as e:
            print(f"Error reading error file: {e}")
        
        return job_id, output, errors, final_status
        
    except Exception as e:
        print(f"Error submitting job: {e}")
        return None, "", "", "FAILED"
    finally:
        # Clean up temp files
        try:
            os.unlink(output_path)
            os.unlink(error_path)
        except:
            pass

def main():
    args = parse_args()
    
    # Create a test command that calculates prime numbers
    test_command = """
    echo "SLURM job started at $(date)"
    echo "Running on host: $(hostname)"
    
    echo "Calculating prime numbers..."
    
    # Simple prime number calculation
    for ((i=2; i<=100; i++)); do
        is_prime=1
        for ((j=2; j<i; j++)); do
            if [ $((i % j)) -eq 0 ]; then
                is_prime=0
                break
            fi
        done
        if [ $is_prime -eq 1 ]; then
            echo "Found prime: $i"
        fi
    done
    
    echo "Done at $(date)"
    """
    
    print("Submitting test job to SLURM...")
    job_id, output, errors, status = run_slurm_job_with_output(test_command, args)
    
    if status == "COMPLETED":
        print("\n=== Job Output ===")
        print(output)
        
        if errors.strip():
            print("\n=== Job Errors ===")
            print(errors)
        else:
            print("\n=== No errors reported ===")
            
        # Count the primes found
        prime_count = output.count("Found prime:")
        print(f"\nFound {prime_count} prime numbers")
        
        return 0
    else:
        print(f"Job failed with status: {status}")
        if errors:
            print("\n=== Job Errors ===")
            print(errors)
        return 1

if __name__ == "__main__":
    sys.exit(main())