#!/usr/bin/env python3
"""
CLI for enhanced background generation, including 'true_random' option.
Wraps `create_background_file_plus` from the `gimmemotifs_plus` extension.

Background Types and Requirements:
  random       - 1st-order Markov sequences based on input FASTA (dinucleotide distribution).
                  Requires --inputfile <FASTA>.
  genomic      - genomic sequences randomly sampled. Requires --genome <GENOME>.
  gc           - genomic sequences matching GC% of input FASTA.
                  Requires both --genome <GENOME> and --inputfile <FASTA>.
  promoter     - random promoter regions. Requires --genome <GENOME> and <genome>.bed file.
  true_random  - truly random sequences. Optionally specify --gc_content (0-1).
"""
import os
import sys
import argparse

# Ensure local src directory is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gimmemotifs_plus.background_plus import create_background_file_plus

def main():
    parser = argparse.ArgumentParser(
        description="Create background FASTA or BED files using GimmeMotifs+, with 'true_random' support."
    )
    parser.add_argument(
        "outfile",
        help="Output file path for background sequences"
    )
    parser.add_argument(
        "-t", "--type",
        dest="bg_type",
        choices=["gc", "genomic", "random", "promoter", "true_random"],
        required=True,
        help="Background type: gc, genomic, random, promoter, or true_random"
    )
    parser.add_argument(
        "-f", "--fmt",
        dest="fmt",
        choices=["fasta", "bed"],
        default="fasta",
        help="Output format (default: fasta)"
    )
    parser.add_argument(
        "-s", "--size",
        dest="size",
        type=int,
        help="Length of generated sequences; default is inferred if not provided"
    )
    parser.add_argument(
        "-g", "--genome",
        dest="genome",
        help="Genome identifier or FASTA for genomic/promoter backgrounds"
    )
    parser.add_argument(
        "-i", "--inputfile",
        dest="inputfile",
        help="Input FASTA or BED file for gc or other background types"
    )
    parser.add_argument(
        "-n", "--number",
        dest="number",
        type=int,
        default=10000,
        help="Number of sequences to generate (default: 10000)"
    )
    parser.add_argument(
        "--gc_content",
        dest="gc_content",
        type=float,
        default=0.5,
        help="GC content for true_random background (0-1, default: 0.5)"
    )

    args = parser.parse_args()

    # Validate required arguments per background type
    if args.bg_type == "random" and not args.inputfile:
        parser.error("'random' background requires --inputfile <FASTA> to build Markov model.")
    if args.bg_type == "genomic" and not args.genome:
        parser.error("'genomic' background requires --genome <GENOME>.")
    # 'gc' accepts either a provided FASTA or a desired GC content, but always needs a genome
    if args.bg_type == "gc":
        if not args.genome or (not args.inputfile and args.gc_content is None):
            parser.error("'gc' background requires --genome <GENOME> and either --inputfile <FASTA> or --gc_content.")
    if args.bg_type == "promoter":
        if not args.genome:
            parser.error("'promoter' background requires --genome <GENOME>.")
    if args.bg_type == "true_random" and not (0 <= args.gc_content <= 1):
        parser.error("'true_random' background requires --gc_content between 0 and 1.")

    create_background_file_plus(
        outfile=args.outfile,
        bg_type=args.bg_type,
        fmt=args.fmt,
        size=args.size,
        genome=args.genome,
        inputfile=args.inputfile,
        number=args.number,
        gc_content=args.gc_content
    )

if __name__ == "__main__":
    main()