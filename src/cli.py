#!/usr/bin/env python3
"""
MotiFab: Motif Fabricator CLI Tool

MotiFab is an artificial data generator designed to produce benchmark datasets for motif 
enrichment tools (e.g., MEME, HOMER). For now, the tool processes a FASTA file containing source 
sequences. In future versions, additional input types (e.g., BED and genome files, along with 
whitelist/blacklist support) will be available.

Key workflow steps (to be integrated later) include:
  1. Selecting a "search set" (target sequences) and a "background set" from the FASTA.
  2. Generating or retrieving a motif (via random generation, a provided motif string, or via a motif file).
  3. Injecting the motif into a subset of the search set (either as an absolute number or a percentage).
  4. Optionally generating background sequences via shuffling.
  
This CLI currently sets up parameter input; later, these parameters will be tied to the complete data-generation workflow.
"""

import argparse

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

    args = parser.parse_args()

    # Debug print: Display all parsed arguments for verification.
    print("Parsed arguments:")
    for arg, value in vars(args).items():
        print(f"  {arg}: {value}")

    # ----------------------------------------------------------------------------
    # Placeholder for the complete data generation workflow.
    #
    # Future integration will include:
    #   1. Loading the FASTA file to retrieve source sequences.
    #   2. Selecting a search set and a background set based on:
    #          - The specified search size.
    #          - The chosen background generation mode ('select' or 'shuffle').
    #   3. Generating or retrieving the motif using one of:
    #          - Random generation (--motif-length)
    #          - A provided motif string (--motif-string)
    #          - A motif file (--motif-file)
    #   4. Injecting the motif into the search set:
    #          - The injection rate will be interpreted as an absolute count or percentage.
    #   5. Optionally shuffling sequences (if background mode is "shuffle") using the specified method.
    #   6. Writing the final search and background datasets to the designated output files.
    #
    # Each step will eventually invoke functions from modules such as:
    #   - fasta_utils.py (for file I/O and sequence selection)
    #   - motif.py (for motif generation/parsing)
    #   - shuffle.py (for sequence shuffling)
    #   - The resolver framework (for modular data processing)
    #
    # For now, this CLI confirms that all parameters have been parsed correctly.
    # ----------------------------------------------------------------------------

    print("\nMotiFab initialized. Data generation steps will be integrated in future development.")

if __name__ == "__main__":
    main()