#!/usr/bin/env python3
"""
Dataset Generator Wrapper for MotiFab

This script generates multiple dataset runs by varying key parameters (such as test set sizes and injection rates)
and calls the existing CLI (src/cli.py) for each combination. For each run, it builds output filenames that include
the parameter values and a run index. It also writes a summary CSV file with one row per run for later analysis.

Usage Example:
--------------
Assuming you have a source FASTA file called random.fasta in your project root, you can run:

  python src/dataset_generator_wrapper.py \
    --input-fasta random.fasta \
    --test-sizes 40,60,100,200 \
    --injection-rates 5%,10%,20%,35%,50%,70% \
    --motif-string AAACCCTTTGGG \
    --background-size 1000 \
    --background-mode shuffle \
    --shuffle-method di-pair \
    --output-dir ./datasets \
    --prefix run \
    --num-runs 1 \
    --summary-file dataset_summary.csv
"""
#TODO add more generation options like motif length, shuffle type, etc. this is very basic due to lack of time.
import os
import sys
import csv
import argparse
import subprocess
from itertools import product

def main():
    parser = argparse.ArgumentParser(
        description="Dataset Generator Wrapper for MotiFab: Generates multiple dataset runs using the CLI."
    )
    # Required input.
    parser.add_argument("--input-fasta", required=True, help="Path to the source FASTA file.")
    # Parameter ranges.
    parser.add_argument("--test-sizes", required=True,
                        help="Comma-separated list of test set sizes (e.g., 40,60,100,200).")
    parser.add_argument("--injection-rates", required=True,
                        help="Comma-separated list of injection rates (e.g., 5%,10%,20%,35%,50%,70%).")
    # Other options.
    parser.add_argument("--motif-string", default="AAACCCTTTGGG",
                        help="Motif string to inject (default: AAACCCTTTGGG).")
    parser.add_argument("--background-size", type=int, default=1000,
                        help="Background set size (default: 1000).")
    parser.add_argument("--background-mode", choices=["select", "shuffle"], default="shuffle",
                        help="Background generation mode (default: shuffle).")
    parser.add_argument("--shuffle-method", choices=["naive", "di-pair"], default="di-pair",
                        help="Shuffle method for background (default: di-pair).")
    parser.add_argument("--output-dir", default=".", help="Directory to place generated dataset files (default: current directory).")
    parser.add_argument("--prefix", default="run",
                        help="Filename prefix for output files (default: run).")
    parser.add_argument("--num-runs", type=int, default=1,
                        help="Number of replicate datasets per parameter combination (default: 1).")
    parser.add_argument("--summary-file", default="dataset_summary.csv",
                        help="CSV file to record parameters and output file names for each run (default: dataset_summary.csv).")
    
    args = parser.parse_args()

    # Convert comma-separated lists.
    try:
        test_sizes = [int(x.strip()) for x in args.test_sizes.split(",")]
    except Exception as e:
        sys.exit("Error parsing test-sizes: " + str(e))
    injection_rates = [x.strip() for x in args.injection_rates.split(",")]

    # Prepare the output directory.
    os.makedirs(args.output_dir, exist_ok=True)
    summary_path = os.path.join(args.output_dir, args.summary_file)
    
    with open(summary_path, 'w', newline='') as csvfile:
        fieldnames = ["run_id", "test_size", "injection_rate", "motif", "background_size",
                      "background_mode", "shuffle_method", "output_search", "output_background"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        run_counter = 1
        # Iterate over all parameter combinations and replicate runs.
        for test_size, inj_rate in product(test_sizes, injection_rates):
            for rep in range(1, args.num_runs + 1):
                # Construct output filenames. Replace '%' with 'pct' for filename safety.
                rate_str = inj_rate.replace("%", "pct")
                out_search = os.path.join(args.output_dir,
                                          f"{args.prefix}_test_{test_size}_{rate_str}_run{rep}.fasta")
                out_background = os.path.join(args.output_dir,
                                              f"{args.prefix}_background_{test_size}_{rate_str}_run{rep}.fasta")
                print(f"\nGenerating dataset #{run_counter}: test_size={test_size}, injection_rate={inj_rate}, run={rep}...")
                # Build the command to call the CLI.
                # We assume that the CLI is implemented in src/cli.py
                cmd = [
                    "python", os.path.join("src", "cli.py"),
                    "--fasta", args.input_fasta,
                    "--motif-string", args.motif_string,
                    "--search-size", str(test_size),
                    "--injection-rate", inj_rate,
                    "--background-size", str(args.background_size),
                    "--background-mode", args.background_mode,
                    "--output-search", out_search,
                    "--output-background", out_background
                ]
                # Pass the shuffle method only if background mode is shuffle.
                if args.background_mode == "shuffle":
                    cmd.extend(["--shuffle-method", args.shuffle_method])
                
                # Run the command.
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode != 0:
                    print(f"Run {run_counter} failed with error:\n{result.stderr}")
                else:
                    print(f"Run {run_counter} succeeded.")
                    print(result.stdout)
                
                # Write the run information to the summary CSV.
                writer.writerow({
                    "run_id": run_counter,
                    "test_size": test_size,
                    "injection_rate": inj_rate,
                    "motif": args.motif_string,
                    "background_size": args.background_size,
                    "background_mode": args.background_mode,
                    "shuffle_method": args.shuffle_method if args.background_mode=="shuffle" else "",
                    "output_search": os.path.basename(out_search),
                    "output_background": os.path.basename(out_background)
                })
                run_counter += 1

    print("\nAll datasets generated. Summary written to:", summary_path)

if __name__ == "__main__":
    main()