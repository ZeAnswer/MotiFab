#!/usr/bin/env python3
"""
FASTA Generation Utilities

This module provides functions for generating random FASTA files and records.
These utilities are standalone and can be used independently of the flowline architecture.
"""

import random
import argparse  # Added for command-line argument parsing

def generate_random_sequence(length: int, gc_content: float | None = None) -> str:  # Changed gc_weights to gc_content
    """
    Generates a random nucleotide sequence of the specified length.
    
    Args:
        length (int): Length of the sequence.
        gc_content (float | None): The desired GC content (proportion of G and C) 
                                     for the sequence, as a float between 0 and 1.
                                     If None, equal weights (0.25 for each) are used.
    
    Returns:
        str: A random sequence composed of A, C, G, and T.
        
    Raises:
        ValueError: If length is not positive or if gc_content is invalid.
    """
    if length <= 0:
        raise ValueError("Sequence length must be positive.")
    nucleotides = "ACGT"
    weights = None
    if gc_content is not None:
        if not (0 <= gc_content <= 1):
            raise ValueError("GC content must be a float between 0 and 1.")
        
        weight_gc = gc_content / 2.0
        weight_at = (1.0 - gc_content) / 2.0
        
        # Order: A, C, G, T
        weights = [weight_at, weight_gc, weight_gc, weight_at]
        
        # Ensure no negative weights if gc_content is exactly 0 or 1
        if gc_content == 0:
            weights = [0.5, 0, 0, 0.5]
        elif gc_content == 1:
            weights = [0, 0.5, 0.5, 0]


    return ''.join(random.choices(nucleotides, weights=weights, k=length))


def generate_random_fasta_records(num_sequences: int = 10,
                                  min_length: int = 50,
                                  max_length: int = 100,
                                  prefix: str = "seq",
                                  gc_content: float | None = None) -> list:  # Changed gc_weights to gc_content
    """
    Generates a list of random FASTA records.
    
    Each record is a dictionary with keys "id", "desc", and "seq". The sequence length 
    for each record is randomly chosen between min_length and max_length (inclusive). The record
    identifier and description are prefixed with the given prefix and an index.
    
    Args:
        num_sequences (int): Number of records to generate (default: 10).
        min_length (int): Minimum sequence length (default: 50).
        max_length (int): Maximum sequence length (default: 100).
        prefix (str): Prefix for the sequence identifier and description (default: "seq").
        gc_content (float | None): Desired GC content. Passed to generate_random_sequence.
    
    Returns:
        list: A list of random FASTA record dictionaries.
    
    Raises:
        ValueError: If min_length is greater than max_length or if parameters are invalid.
    """
    if num_sequences <= 0:
        raise ValueError("Number of sequences must be positive.")
    if min_length <= 0:
        raise ValueError("Minimum sequence length must be positive.")
    if max_length <= 0:
        raise ValueError("Maximum sequence length must be positive.")
    if min_length > max_length:
        raise ValueError("min_length cannot be greater than max_length.")
        
    records = []
    for i in range(1, num_sequences + 1):
        length = random.randint(min_length, max_length)
        seq = generate_random_sequence(length, gc_content=gc_content)  # Pass gc_content
        record_id = f"{prefix}{i}"
        record_desc = f"{prefix}{i}"  # For simplicity, using the same for id and description.
        record = {"id": record_id, "desc": record_desc, "seq": seq}
        records.append(record)
    return records


def generate_random_fasta_file(output_path: str,
                               num_sequences: int = 10,
                               min_length: int = 50,
                               max_length: int = 100,
                               prefix: str = "seq",
                               gc_content: float | None = None) -> None:  # Changed gc_weights to gc_content
    """
    Generates a random FASTA file with the given parameters.
    
    Args:
        output_path (str): The path to write the generated FASTA file.
        num_sequences (int): Number of sequences to generate (default: 10).
        min_length (int): Minimum sequence length (default: 50).
        max_length (int): Maximum sequence length (default: 100).
        prefix (str): Prefix for each sequence identifier (default: "seq").
        gc_content (float | None): Desired GC content. Passed to generate_random_fasta_records.
        
    Raises:
        ValueError: If parameters are invalid or if the file cannot be written.
    """
    records = generate_random_fasta_records(num_sequences, min_length, max_length, prefix, gc_content=gc_content)  # Pass gc_content
    
    try:
        with open(output_path, 'w') as f:
            for record in records:
                f.write(f">{record['desc']}\n")
                seq = record["seq"]
                # Write sequence in lines of 80 characters
                for i in range(0, len(seq), 80):
                    f.write(seq[i:i+80] + "\n")
    except IOError as e:
        raise ValueError(f"Could not write to file {output_path}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a random FASTA file with specified GC content.")
    parser.add_argument("output_path", type=str, help="The path to write the generated FASTA file.")
    parser.add_argument("--num_sequences", type=int, default=10, help="Number of sequences to generate (default: 10).")
    parser.add_argument("--min_length", type=int, default=50, help="Minimum sequence length (default: 50).")
    parser.add_argument("--max_length", type=int, default=100, help="Maximum sequence length (default: 100).")
    parser.add_argument("--prefix", type=str, default="seq", help="Prefix for each sequence identifier (default: \"seq\").")
    parser.add_argument("--gc_content", type=float, metavar='GC_PROPORTION',
                        help="Desired GC content (proportion of G and C) as a float between 0 and 1 (e.g., 0.6 for 60%% GC). "
                             "If not provided, equal weights (0.25 for each nucleotide) are used.")

    args = parser.parse_args()

    try:
        generate_random_fasta_file(
            output_path=args.output_path,
            num_sequences=args.num_sequences,
            min_length=args.min_length,
            max_length=args.max_length,
            prefix=args.prefix,
            gc_content=args.gc_content
        )
        print(f"Successfully generated FASTA file: {args.output_path}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")