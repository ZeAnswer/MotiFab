#!/usr/bin/env python3
"""
FASTA Generation Utilities

This module provides functions for generating random FASTA files and records.
These utilities are standalone and can be used independently of the flowline architecture.
"""

import random

def generate_random_sequence(length: int) -> str:
    """
    Generates a random nucleotide sequence of the specified length.
    
    Args:
        length (int): Length of the sequence.
    
    Returns:
        str: A random sequence composed of A, C, G, and T.
        
    Raises:
        ValueError: If length is not positive.
    """
    if length <= 0:
        raise ValueError("Sequence length must be positive.")
    nucleotides = "ACGT"
    return ''.join(random.choices(nucleotides, k=length))


def generate_random_fasta_records(num_sequences: int = 10,
                                  min_length: int = 50,
                                  max_length: int = 100,
                                  prefix: str = "seq") -> list:
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
        seq = generate_random_sequence(length)
        record_id = f"{prefix}{i}"
        record_desc = f"{prefix}{i}"  # For simplicity, using the same for id and description.
        record = {"id": record_id, "desc": record_desc, "seq": seq}
        records.append(record)
    return records


def generate_random_fasta_file(output_path: str,
                               num_sequences: int = 10,
                               min_length: int = 50,
                               max_length: int = 100,
                               prefix: str = "seq") -> None:
    """
    Generates a random FASTA file with the given parameters.
    
    Args:
        output_path (str): The path to write the generated FASTA file.
        num_sequences (int): Number of sequences to generate (default: 10).
        min_length (int): Minimum sequence length (default: 50).
        max_length (int): Maximum sequence length (default: 100).
        prefix (str): Prefix for each sequence identifier (default: "seq").
        
    Raises:
        ValueError: If parameters are invalid or if the file cannot be written.
    """
    records = generate_random_fasta_records(num_sequences, min_length, max_length, prefix)
    
    try:
        with open(output_path, 'w') as f:
            for record in records:
                f.write(f">{record['desc']}\n")
                seq = record["seq"]
                # Write sequence in lines of 80 characters
                for i in range(0, len(seq), 80):
                    f.write(seq[i:i+80] + "\n")
    except Exception as e:
        raise ValueError(f"Error writing FASTA file to '{output_path}': {str(e)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate a random FASTA file with specified parameters."
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Output path for the generated FASTA file."
    )
    parser.add_argument(
        "--num-sequences", "-n",
        type=int,
        default=10,
        help="Number of sequences to generate (default: 10)."
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=50,
        help="Minimum sequence length (default: 50)."
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=100,
        help="Maximum sequence length (default: 100)."
    )
    parser.add_argument(
        "--prefix", "-p",
        default="seq",
        help="Prefix for sequence identifiers (default: 'seq')."
    )
    
    args = parser.parse_args()
    
    print(f"Generating random FASTA file with {args.num_sequences} sequences...")
    generate_random_fasta_file(
        args.output,
        args.num_sequences,
        args.min_length,
        args.max_length,
        args.prefix
    )
    print(f"Successfully generated FASTA file at: {args.output}")