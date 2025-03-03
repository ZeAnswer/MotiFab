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

from flowline import (LoadFastaPipe, WriteFastaPipe, SelectRandomFastaSequencesPipe,
                      GenerateRandomMotifsPipe, ProcessProvidedMotifPipe, ParsePWMPipe, SampleMotifsFromPWMPipe,
                      NaiveShufflePipe, DiPairShufflePipe, InjectMotifsIntoFastaRecordsPipe,
                      UnitAmountConverterPipe, build_flow, FlowOutputRenamer)

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

    # Build and run the flow
    try:
        run_flow(args)
    except Exception as e:
        sys.exit(f"Error: {e}")

def run_flow(args):
    """
    Build and run the MotiFab flow based on command-line arguments.
    """
    # Define flow configuration
    flow_config = {}
    
    # Add the load FASTA pipe
    flow_config['load_fasta'] = {
        'type': LoadFastaPipe,
        'init': {},
        'upstream_pipes': {
            '*': {'fasta_file_path': 'fasta_file_path'}
        }
    }
    
    # Add the search set selection pipe
    flow_config['select_search'] = {
        'type': SelectRandomFastaSequencesPipe,
        'init': {},
        'upstream_pipes': {
            'load_fasta': {'fasta_records': 'fasta_records'},
            '*': {'search_size': 'amount'}
        }
    }
    
    # Add motif generation based on input
    if args.motif_length is not None:
        # Random motif generation
        flow_config['motif_generator'] = {
            'type': GenerateRandomMotifsPipe,
            'init': {
                'amount': 1, 
                'length': args.motif_length
            },
            'upstream_pipes': {}
        }
    elif args.motif_string:
        # Process provided motif string
        flow_config['motif_generator'] = {
            'type': ProcessProvidedMotifPipe,
            'init': {},
            'upstream_pipes': {
                '*': {'motif_string': 'motif_string'}
            }
        }
    elif args.motif_file:
        # PWM-based motif generation
        flow_config['parse_pwm'] = {
            'type': ParsePWMPipe,
            'init': {},
            'upstream_pipes': {
                '*': {'pwm_file_path': 'pwm_file_path'}
            }
        }
        flow_config['motif_generator'] = {
            'type': SampleMotifsFromPWMPipe,
            'init': {},
            'upstream_pipes': {
                'parse_pwm': {'pwm_matrix': 'pwm_matrix'},
                'injection_converter': {'injection_rate': 'amount'}
            }
        }
    
    # Add injection converter pipe
    flow_config['injection_converter'] = {
        'type': UnitAmountConverterPipe,
        'init': {},
        'upstream_pipes': {
            'select_search': {'fasta_records': 'items'},
            '*': {'injection_rate': 'amount'}
        }
    }
    
    # Add motif injection pipe
    flow_config['inject_motifs'] = {
        'type': InjectMotifsIntoFastaRecordsPipe,
        'init': {},
        'upstream_pipes': {
            'select_search': {'fasta_records': 'fasta_records'},
            'motif_generator': {'motif_strings': 'motif_strings'},
            'injection_converter': {'amount': 'amount'}
        }
    }
    
    # Add search set output pipe
    flow_config['write_search'] = {
        'type': WriteFastaPipe,
        'init': {},
        'upstream_pipes': {
            'inject_motifs': {'fasta_records': 'fasta_records'},
            '*': {'output_search_path': 'fasta_file_path'}
        }
    }
    
    # Add search set output renamer
    flow_config['search_output_renamer'] = {
        'type': FlowOutputRenamer,
        'init': {'output_mapping': {'write_success': 'search_write_success'}},
        'upstream_pipes': {'write_search': {'write_success': 'write_success'}}
    }
    
    # Add background generation based on the selected mode
    if args.background_mode == 'select':
        # Selection mode: select from remaining FASTA records
        flow_config['select_background'] = {
            'type': SelectRandomFastaSequencesPipe,
            'init': {},
            'upstream_pipes': {
                'load_fasta': {'fasta_records': 'fasta_records'},
                'select_search': {'indices': 'excluded_indices'},
                '*': {'background_size': 'amount'}
            }
        }
        
        # Background output pipe
        flow_config['write_background'] = {
            'type': WriteFastaPipe,
            'init': {},
            'upstream_pipes': {
                'select_background': {'fasta_records': 'fasta_records'},
                '*': {'output_background_path': 'fasta_file_path'}
            }
        }
    else:  # 'shuffle' mode
        # Use the appropriate shuffle method
        shuffle_pipe_type = NaiveShufflePipe if args.shuffle_method == 'naive' else DiPairShufflePipe
        
        # Select from the search set for shuffling (can be greater than search size)
        flow_config['select_background'] = {
            'type': SelectRandomFastaSequencesPipe,
            'init': {},
            'upstream_pipes': {
                'select_search': {'fasta_records': 'fasta_records'},
                '*': {'background_size': 'amount'}
            }
        }
        
        # Shuffle pipe
        flow_config['shuffle_search'] = {
            'type': shuffle_pipe_type,
            'init': {},
            'upstream_pipes': {
                'select_background': {'fasta_records': 'fasta_records'}
            }
        }
           
        # Background output pipe
        flow_config['write_background'] = {
            'type': WriteFastaPipe,
            'init': {},
            'upstream_pipes': {
                'shuffle_search': {'fasta_records': 'fasta_records'},
                '*': {'output_background_path': 'fasta_file_path'}
            }
        }
    
    # Add background output renamer
    flow_config['background_output_renamer'] = {
        'type': FlowOutputRenamer,
        'init': {'output_mapping': {'write_success': 'background_write_success'}},
        'upstream_pipes': {'write_background': {'write_success': 'write_success'}}
    }
        
    
    # Prepare external inputs
    external_inputs = {
        'fasta_file_path': args.fasta,
        'search_size': args.search_size,
        'background_size': args.background_size,
        'injection_rate': args.injection_rate,
        'output_search_path': args.output_search,
        'output_background_path': args.output_background
    }
    
    # Add any motif-specific inputs
    if args.motif_string:
        external_inputs['motif_string'] = args.motif_string
    elif args.motif_file:
        external_inputs['pwm_file_path'] = args.motif_file
        
    # Build the flow
    manager, pipes = build_flow(flow_config)
    
    # Run the flow with external inputs
    try:
        manager.validate_flow()
        result = manager.run(external_inputs)
        
        # Report success and output locations
        print("Search set (test set) written to:", args.output_search)
        print("Background set written to:", args.output_background)
        
    except Exception as e:
        raise RuntimeError(f"Flow execution failed: {e}")

if __name__ == "__main__":
    main()