#!/usr/bin/env python3
"""
Test script for FastaPlus.inject_motif functionality.
"""
import random
from gimmemotifs_plus.fasta_plus import FastaPlus
from gimmemotifs_plus.motif_plus import MotifPlus

def main():
    # For reproducibility
    random.seed(42)

    print("===== Test 1: injection_rate =====")
    # Create random FASTA with 10 sequences, all GC (100% GC)
    f = FastaPlus()
    f.populate_random_fasta(num_sequences=10, min_length=20, max_length=20, prefix="seq", gc_content=1.0)

    # Simple motif AAA TTT
    motif1 = MotifPlus(consensus="AAATTT")
    f.inject_motif(motif1, injection_rate=0.3)

    # Count sequences with injected As (motif presence)
    count = sum(1 for seq in f.seqs if 'A' in seq)
    expected = int(round(0.3 * 10))
    print(f"Injected into {count} sequences (expected ~{expected})")
    for i, seq in enumerate(f.seqs, start=1):
        print(f"seq{i}: {seq}")

    print("\n===== Test 2: injection_amount =====")
    # New random FASTA
    f2 = FastaPlus()
    f2.populate_random_fasta(num_sequences=10, min_length=20, max_length=20, prefix="seq", gc_content=1.0)

    # Consensus motif with mutation rate
    motif2 = MotifPlus(consensus="AAAAAAAAAA", mutation_rate=0.4)
    f2.inject_motif(motif2, injection_amount=5)

    # Count sequences with injected As
    count2 = sum(1 for seq in f2.seqs if 'A' in seq)
    print(f"Injected into {count2} sequences (expected 5)")
    for i, seq in enumerate(f2.seqs, start=1):
        print(f"seq{i}: {seq}")

if __name__ == "__main__":
    main()
