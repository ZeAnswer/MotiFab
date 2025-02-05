#!/usr/bin/env python3
"""
MotiFab: Motif Fabricator CLI Tool

MotiFab is an artificial data generator designed to produce benchmark datasets for motif 
enrichment tools (e.g., MEME, HOMER). For now, the tool processes a FASTA file containing source 
sequences and generates two output FASTA files:
  - A search set (test set) with motif injections.
  - A background set, generated either by selecting non-test sequences or by shuffling the test set.

Workflow:
  1. Load the input FASTA file.
  2. Select a search set of the specified size.
  3. Generate a background set according to the chosen mode.
  4. Generate a motif (from a provided string, random length, or PWM file).
  5. Determine the injection count (absolute or percentage).
  6. Inject the motif into the chosen search set sequences.
  7. Write the search and background sets to output FASTA files.
  
  A new flag, --dry-run, is added so that in testing the CLI only parses and prints the parameters.
"""

import argparse
import random
import sys

from src.fasta_utils import load_fasta, write_fasta, select_random_sequences
from src.motif import Motif
from src.shuffle import shuffle_sequence
from src.sequence_injector import inject_motif_into_records

def main():
    parser = argparse.ArgumentParser(
        description="MotiFab: Generate artificial benchmark datasets for motif enrichment tools using a FASTA file."
    )

    # Input FASTA file: for now, this is the only allowed input.
    parser.add_argument(
        "--fasta",
        required=True,
        help="Path to the input FASTA file containing source sequences."
    )

    # Motif specification: exactly one of these must be provided.
    motif_group = parser.add_mutually_exclusive_group(required=True)
    motif_group.add_argument(
        "--motif-length",
        type=int,
        help="Length of a random motif to generate (e.g., 10)."
    )
    motif_group.add_argument(
        "--motif-string",
        help="A specific motif string to inject (e.g., 'ACGTACGT')."
    )
    motif_group.add_argument(
        "--motif-file",
        help="Path to a file containing a motif (or PWM) to be used."
    )

    # Parameters for sequence selection from the FASTA file.
    parser.add_argument(
        "--search-size",
        type=int,
        required=True,
        help="Number of sequences to include in the search set. (Required)"
    )
    parser.add_argument(
        "--background-size",
        type=int,
        default=1000,
        help="Number of sequences to include in the background set (default: 1000)."
    )
    
    # Injection rate: accepts an absolute number or a percentage string (e.g., "10%").
    parser.add_argument(
        "--injection-rate",
        required=True,
        help="Specify the injection rate as either an absolute number or a percentage (e.g., '10%') "
             "to determine how many sequences in the search set should have the motif injected. (Required)"
    )

    # Options for generating the background set.
    # 'select' mode uses sequences from the FASTA that are not in the search set.
    # 'shuffle' mode applies shuffling to the search set to generate additional sequences.
    parser.add_argument(
        "--background-mode",
        choices=["select", "shuffle"],
        default="select",
        help="Method to generate background sequences: 'select' (use remaining sequences) or "
             "'shuffle' (apply shuffling to the search set; default: select)."
    )
    parser.add_argument(
        "--shuffle-method",
        choices=["naive", "di-pair"],
        default="naive",
        help="If background mode is 'shuffle', specify the shuffling method: 'naive' or 'di-pair' (default: naive)."
    )

    # Optional output file paths for the generated datasets.
    parser.add_argument(
        "--output-search",
        default="search_set.fasta",
        help="Output file for the search set with injected motifs (default: search_set.fasta)."
    )
    parser.add_argument(
        "--output-background",
        default="background_set.fasta",
        help="Output file for the background sequences (default: background_set.fasta)."
    )

    # New flag for dry-run mode: if set, the CLI stops after parsing and printing arguments.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, perform a dry run (only parse and print arguments; no file I/O)."
    ) 

    args = parser.parse_args()

    # Debug print: Display all parsed arguments for verification.
    print("Parsed arguments:")
    for arg, value in vars(args).items():
        print(f"  {arg}: {value}")

    # If dry-run is set, exit without performing any further actions.
    if args.dry_run:
        print("Dry run mode: no further action performed.")
        sys.exit(0) 

    # Step 1: Load the input FASTA file.
    try:
        records = load_fasta(args.fasta)
    except Exception as e:
        sys.exit(f"Error loading FASTA file: {e}")

    if args.search_size > len(records):
        sys.exit("Error: search-size exceeds the number of sequences in the input FASTA file.")

    # Step 2: Select the search set.
    search_set = select_random_sequences(records, args.search_size)

    # Step 3: Generate the background set.
    if args.background_mode == "select":
        search_ids = {rec["id"] for rec in search_set}
        background_candidates = [rec for rec in records if rec["id"] not in search_ids]
        if len(background_candidates) < args.background_size:
            background_set = background_candidates
        else:
            background_set = select_random_sequences(background_candidates, args.background_size)
    elif args.background_mode == "shuffle":
        # Generate background set by shuffling each record in the search set repeatedly until
        # we have enough background sequences.
        background_set = []
        bg_counter = 1  # Initialize a counter for unique background IDs.
        while len(background_set) < args.background_size:
            for rec in search_set:
                new_rec = rec.copy()
                new_rec["seq"] = shuffle_sequence(rec["seq"], method=args.shuffle_method)
                # Append a unique suffix to both the id and description.
                new_rec["id"] = f"{new_rec['id']}_bg{bg_counter}"
                new_rec["desc"] = f"{new_rec['desc']}_bg{bg_counter}"
                bg_counter += 1
                background_set.append(new_rec)
                if len(background_set) >= args.background_size:
                    break
    else:
        sys.exit("Unknown background mode.")

    # Step 4: Generate the motif.
    if args.motif_string:
        motif_obj = Motif(args.motif_string, input_type="string")
    elif args.motif_length is not None:
        motif_obj = Motif(args.motif_length, input_type="length")
    elif args.motif_file:
        motif_obj = Motif(args.motif_file, input_type="file")
    else:
        sys.exit("No motif option provided.")
    motif_value = motif_obj.get_motif()

    # Step 5: Inject the motif into the search set (using the sequence_injector module).
    injected_search_set = inject_motif_into_records(search_set, motif_value, args.injection_rate)

    # Step 6: Write the output FASTA files.
    try:
        write_fasta(injected_search_set, args.output_search)
        write_fasta(background_set, args.output_background)
    except Exception as e:
        sys.exit(f"Error writing output files: {e}")

    print("Search set (test set) written to:", args.output_search)
    print("Background set written to:", args.output_background)

if __name__ == "__main__":
    main()