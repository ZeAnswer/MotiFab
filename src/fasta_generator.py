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

# Ensure local src directory is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gimmemotifs_plus.background_plus import create_background_file_plus
from dataset_manager import DatasetManager

class FastaGenerator:
    """
    Generates background FASTA/BED using parameters from DatasetManager.
    """
    def __init__(self, dataset_manager: DatasetManager):
        self.dm = dataset_manager
        self.params = self.dm.get_fasta_generation_params()

    def generate(self):
        """Call create_background_file_plus with JSON-driven parameters."""
        create_background_file_plus(**self.params)
        # record the generated background as the new 'master_fasta'
        outfile = self.params.get('outfile')
        if outfile:
            # update dataset_generation_params.master_fasta in the JSON
            dgen = self.dm.get_dataset_generation_params()
            dgen['master_fasta'] = outfile
            self.dm.update_dataset_generation_params(dgen)
        return

if __name__ == "__main__":
    # JSON-driven background FASTA/BED generation
    config_path = "/polio/oded/MotiFabEnv/presentation_run/dataset_config.json"
    dm = DatasetManager(config_path)
    fg = FastaGenerator(dm)
    outfile = fg.generate()
    print(f"Background file generated: {outfile}")
