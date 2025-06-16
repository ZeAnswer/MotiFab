#!/usr/bin/env python3
"""
End-to-end driver for the MotiFab workflow:
1. Generate background FASTA
2. Generate injected/test datasets
3. Run de-novo motif discovery
4. Parse results and dump CSVs
5. Produce heatmaps

Usage:
    python run_motifab.py /path/to/dataset_config.json
"""
import os
import sys
import argparse

# Ensure local src directory is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dataset_manager import DatasetManager
from fasta_generator import FastaGenerator
from dataset_generator import DatasetGenerator
from denovo_runner import DenovoRunner
from results_parser import ResultsParser
from result_heatmaps import HeatmapGenerator


def main():
    parser = argparse.ArgumentParser(description="Run full MotiFab workflow")
    parser.add_argument('config', help="Path to JSON config file")
    args = parser.parse_args()

    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    print(f"Loading configuration: {config_path}")
    dm = DatasetManager(config_path)

    # Step 1: Background FASTA generation
    print("[1/5] Generating background FASTA...")
    fg = FastaGenerator(dm)
    fg.generate()

    # Step 2: Dataset generation
    print("[2/5] Generating datasets...")
    dg = DatasetGenerator(dm)
    dg.generate_datasets()

    # Step 3: De-novo motif discovery
    print("[3/5] Running de-novo motif discovery...")
    dr = DenovoRunner(dm)
    dr.run_denovo()

    # Step 4: Results parsing
    print("[4/5] Parsing results...")
    rp = ResultsParser(dm)
    rp.run_all()

    # Step 5: Heatmap generation
    print("[5/5] Generating heatmaps...")
    hg = HeatmapGenerator(dm)
    hg.generate()

    print("MotiFab workflow complete.")


if __name__ == '__main__':
    main()
