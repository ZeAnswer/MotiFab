#!/usr/bin/env python3
"""
MotiFab: Motif Dataset Generator

This tool generates benchmark datasets for motif enrichment tools (e.g., MEME, HOMER).
It creates pairs of FASTA files containing:
  1. A search set (test set) with motif injections
  2. A background set, generated either by selecting non-test sequences or by shuffling the test set

Features:
  - Inject motifs from different sources: random generation, string input, or PWM file
  - Generate single datasets or parameter sweeps with multiple combinations
  - Customize test sizes, injection rates, and background generation methods
  - Export summary information for further analysis
  - Support for configuration via INI files

Basic Usage:
  python src/motif_dataset_generator.py --fasta input.fasta --motif-string ACGTACGT --search-size 100 --injection-rate 10%

Parameter Sweep Usage:
  python src/motif_dataset_generator.py --fasta input.fasta --motif-string ACGTACGT 
      --test-sizes 40,60,100,200 --injection-rates 5%,10%,20% --replicates 3
      --output-dir ./datasets --summary-file summary.csv

Config File Usage:
  python src/motif_dataset_generator.py --config my_config.ini
  (Command line arguments override config file settings)
"""
#TODO need to make sure that all args.something are okay and won't cause a crash. if we are not sure something exists, it must be checked first with hasattr(args, 'something')
import os
import sys
import csv
import argparse
import random
import configparser
import shutil
from pathlib import Path
from itertools import product

from flowline import (FlowManager, FlowSource, FlowPipe, FlowOutputFilter,
                      LoadFastaPipe, WriteFastaPipe, SelectRandomFastaSequencesPipe,
                      GenerateRandomMotifsPipe, ProcessProvidedMotifPipe, ParsePWMPipe, SampleMotifsFromPWMPipe,
                      NaiveShufflePipe, DiPairShufflePipe, InjectMotifsIntoFastaRecordsPipe,
                      UnitAmountConverterPipe, FlowOutputRenamer, build_flow)


def copy_pwm_to_output_dir(pwm_file_path, output_dir):
    """
    Copy a PWM file to the output directory.
    
    Args:
        pwm_file_path (str): Original path to the PWM file
        output_dir (str): Directory to copy the PWM file to
        
    Returns:
        str: Filename of the copied PWM file (without path)
    """
    if not os.path.isfile(pwm_file_path):
        raise FileNotFoundError(f"PWM file not found: {pwm_file_path}")
    
    # Get just the filename without path
    pwm_filename = os.path.basename(pwm_file_path)
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Destination path
    dest_path = os.path.join(output_dir, pwm_filename)
    
    # Copy the file
    shutil.copy2(pwm_file_path, dest_path)
    
    return pwm_filename


def build_motif_dataset_flow(args):
    """
    Build the flow for generating motif datasets.
    
    Args:
        args: A namespace containing CLI arguments needed for flow construction
        
    Returns:
        tuple: (FlowManager, dict) - The flow manager and the pipes dictionary
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
    if hasattr(args, "motif_length") and args.motif_length is not None:
        # Random motif generation
        flow_config['motif_generator'] = {
            'type': GenerateRandomMotifsPipe,
            'init': {
                'amount': 1, 
                'length': args.motif_length
            },
            'upstream_pipes': {}
        }
    elif hasattr(args, "motif_string") and args.motif_string:
        # Process provided motif string
        flow_config['motif_generator'] = {
            'type': ProcessProvidedMotifPipe,
            'init': {},
            'upstream_pipes': {
                '*': {'motif_string': 'motif_string'}
            }
        }
    elif hasattr(args, "motif_file") and args.motif_file:
        # PWM-based motif generation
        flow_config['parse_pwm'] = {
            'type': ParsePWMPipe,
            'init': {'pwm_file_path': args.motif_file},
            'upstream_pipes': {}
        }
        flow_config['motif_generator'] = {
            'type': SampleMotifsFromPWMPipe,
            'init': {},
            'upstream_pipes': {
                'parse_pwm': {'pwm_matrix': 'pwm_matrix'},
                'injection_converter': {'amount': 'amount'}
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
    if hasattr(args, "background_mode") and args.background_mode == 'select':
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
        
    # Build the flow
    manager, pipes = build_flow(flow_config)
    
    # Validate the flow
    manager.initialize_and_validate_flow()
    
    return manager, pipes


def run_dataset_generator(flow_manager, dataset_params):
    """
    Run the dataset generator flow with the specified parameters.
    
    Args:
        flow_manager (FlowManager): The pre-built flow manager
        dataset_params (dict): Parameters for this specific dataset run
        
    Returns:
        dict: Flow execution results
    """
    try:
        result = flow_manager.run(dataset_params)
        
        # Report success
        print(f"Success! Generated dataset with {dataset_params['search_size']} sequences " + 
              f"and {dataset_params['injection_rate']} motif injections")
        print(f"Search set written to: {dataset_params['output_search_path']}")
        print(f"Background set written to: {dataset_params['output_background_path']}")
        
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None


def generate_multiple_datasets(flow_manager, args):
    """
    Generate multiple datasets based on parameter ranges.
    
    Args:
        flow_manager (FlowManager): The pre-built flow manager
        args: Arguments containing parameter ranges
        
    Returns:
        bool: True if all datasets were generated successfully
    """
    # Parse parameter ranges
    try:
        test_sizes = [int(x.strip()) for x in args.test_sizes.split(",")]
    except Exception as e:
        print(f"Error parsing test-sizes: {e}")
        return False
    
    injection_rates = [x.strip() for x in args.injection_rates.split(",")]
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    summary_path = os.path.join(args.output_dir, args.summary_file)
    
    # If using a PWM file, copy it to the output directory
    pwm_filename = None
    if hasattr(args, "motif_file") and args.motif_file:
        try:
            pwm_filename = copy_pwm_to_output_dir(args.motif_file, args.output_dir)
            print(f"Copied PWM file to output directory: {pwm_filename}")
        except Exception as e:
            print(f"Error copying PWM file: {e}")
            return False
    
    # Create a summary CSV file
    with open(summary_path, 'w', newline='') as csvfile:
        fieldnames = [
            "run_id", "test_size", "injection_rate", "motif", 
            "background_size", "background_mode", "shuffle_method", 
            "output_search", "output_background"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        combination_id = 1
        success_count = 0
        total_count = len(test_sizes) * len(injection_rates) * args.replicates
        
        # Determine the motif description for the summary
        if hasattr(args, "motif_string") and args.motif_string:
            motif_description = args.motif_string
        elif hasattr(args, "motif_length") and args.motif_length:
            motif_description = f"random_{args.motif_length}bp"
        elif pwm_filename:
            motif_description = pwm_filename
        else:
            motif_description = "from_file"
        
        # Iterate over all parameter combinations
        for test_size, injection_rate in product(test_sizes, injection_rates):
            # For each parameter combination, use a new combination ID
            for replicate in range(1, args.replicates + 1):
                # Create safe filename strings (replace % with pct)
                rate_str = injection_rate.replace("%", "pct")
                
                # Create a two-level run ID (combination_id_replicate_id)
                run_id = f"{combination_id}_{replicate}"
                
                # Generate filenames
                search_filename = f"{args.prefix}_test_{test_size}_{rate_str}_run_{run_id}.fasta"
                bg_filename = f"{args.prefix}_background_{test_size}_{rate_str}_run_{run_id}.fasta"
                output_search = os.path.join(args.output_dir, search_filename)
                output_background = os.path.join(args.output_dir, bg_filename)
                
                print(f"\nGenerating dataset #{run_id} (Combination #{combination_id}, Replicate #{replicate}):")
                print(f"  Test size: {test_size}, Injection rate: {injection_rate}")
                
                # Create parameters for this specific run
                dataset_params = {
                    'fasta_file_path': args.fasta,
                    'search_size': test_size,
                    'injection_rate': injection_rate,
                    'background_size': args.background_size,
                    'output_search_path': output_search,
                    'output_background_path': output_background
                }
                
                # Add motif-specific parameters
                if hasattr(args, "motif_string") and args.motif_string:
                    dataset_params['motif_string'] = args.motif_string
                elif hasattr(args, "motif_file") and args.motif_file:
                    dataset_params['pwm_file_path'] = args.motif_file
                
                # Run the flow with these parameters
                result = run_dataset_generator(flow_manager, dataset_params)
                
                if result:
                    success_count += 1
                    # Write to the summary CSV
                    writer.writerow({
                        "run_id": run_id,
                        "test_size": test_size,
                        "injection_rate": injection_rate,
                        "motif": motif_description,
                        "background_size": args.background_size,
                        "background_mode": args.background_mode,
                        "shuffle_method": args.shuffle_method if args.background_mode == "shuffle" else "",
                        "output_search": search_filename,
                        "output_background": bg_filename
                    })
                
                # Explicitly flush the CSV file to ensure all rows are written
                csvfile.flush()
            
            # Increment combination ID after all replicates of a combination are processed
            combination_id += 1
    
    print(f"\nDataset generation complete: {success_count}/{total_count} successful")
    print(f"Summary written to: {summary_path}")
    return success_count == total_count


def load_config_file(config_file):
    """
    Load configuration from an INI file.

    Args:
        config_file (str): Path to the INI file

    Returns:
        dict: Dictionary containing configuration parameters
    """
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        
        # Convert the config to a dictionary
        config_dict = {}
        
        # Process the DEFAULT section if it exists
        if 'DEFAULT' in config:
            for key, value in config['DEFAULT'].items():
                config_dict[key.replace('-', '_')] = value
                
        # Process the specific sections
        for section in config.sections():
            for key, value in config[section].items():
                # Convert hyphens in key names to underscores for consistency with argparse
                config_dict[key.replace('-', '_')] = value
                
        return config_dict
    except Exception as e:
        print(f"Error loading configuration file {config_file}: {e}")
        sys.exit(1)


def parse_args():
    """
    Parse command-line arguments and optionally load from a configuration file.
    Command-line arguments override config file values.
    
    Returns:
        argparse.Namespace: The parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="MotiFab: Generate benchmark datasets for motif enrichment tools"
    )
    
    # Configuration file option
    parser.add_argument(
        "--config",
        help="Path to a configuration INI file containing parameters"
    )
    
    # Input FASTA file
    parser.add_argument(
        "--fasta",
        help="Path to the input FASTA file containing source sequences"
    )
    
    # Motif specification: exactly one of these must be provided
    motif_group = parser.add_mutually_exclusive_group()
    motif_group.add_argument(
        "--motif-length",
        type=int,
        help="Length of a random motif to generate (e.g., 10)"
    )
    motif_group.add_argument(
        "--motif-string",
        help="A specific motif string to inject (e.g., 'ACGTACGT')"
    )
    motif_group.add_argument(
        "--motif-file",
        help="Path to a file containing a motif (or PWM) to use"
    )
    
    # Parameter sweep mode options
    sweep_group = parser.add_argument_group("Parameter sweep options")
    sweep_group.add_argument(
        "--test-sizes", 
        help="Comma-separated list of test set sizes for parameter sweep (e.g., '40,60,100,200')"
    )
    sweep_group.add_argument(
        "--injection-rates",
        help="Comma-separated list of injection rates for parameter sweep (e.g., '5%%,10%%,20%%')"
    )
    sweep_group.add_argument(
        "--replicates",
        type=int,
        help="Number of replicate datasets to generate per parameter combination (default: 1)"
    )
    sweep_group.add_argument(
        "--output-dir",
        help="Directory to store output files for parameter sweep (default: current directory)"
    )
    sweep_group.add_argument(
        "--prefix",
        help="Filename prefix for output files in parameter sweep (default: 'run')"
    )
    sweep_group.add_argument(
        "--summary-file",
        help="CSV file to record parameters and filenames (default: 'dataset_summary.csv')"
    )
    
    # Single dataset mode options
    single_group = parser.add_argument_group("Single dataset options")
    single_group.add_argument(
        "--search-size",
        type=int,
        help="Number of sequences in the search set (required for single dataset)"
    )
    single_group.add_argument(
        "--injection-rate",
        help="Injection rate as absolute number or percentage (e.g., '10%%', required for single dataset)"
    )
    single_group.add_argument(
        "--output-search",
        help="Output file path for search set (default: 'search_set.fasta')"
    )
    single_group.add_argument(
        "--output-background",
        help="Output file path for background set (default: 'background_set.fasta')"
    )
    
    # Common options
    parser.add_argument(
        "--background-size",
        type=int,
        help="Number of sequences in the background set (default: 1000)"
    )
    parser.add_argument(
        "--background-mode",
        choices=["select", "shuffle"],
        help="Method for generating background: 'select' (use remaining sequences) or "
             "'shuffle' (shuffle search sequences; default: select)"
    )
    parser.add_argument(
        "--shuffle-method",
        choices=["naive", "di-pair"],
        help="If background mode is 'shuffle', specify method: 'naive' or 'di-pair' (default: naive)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse arguments without generating datasets (for testing)"
    )
    
    # First parse the command line arguments
    args = parser.parse_args()
    
    # If a config file is specified, load it and merge with command line arguments
    if args.config:
        config_dict = load_config_file(args.config)
        
        # Create a new namespace with config file values
        config_args = argparse.Namespace()
        
        # Set defaults from the config file
        for key, value in config_dict.items():
            # Special case for the dry-run flag (which is a boolean)
            if key == 'dry_run':
                if value.lower() in ('true', 'yes', '1', 'on'):
                    setattr(config_args, key, True)
                continue
            
            # Handle integer values
            if key in ('motif_length', 'search_size', 'background_size', 'replicates'):
                try:
                    setattr(config_args, key, int(value))
                except ValueError:
                    print(f"Warning: Could not convert {key}={value} to integer in config file. Using default.")
            else:
                setattr(config_args, key, value)
        
        # Override with command line arguments (any that were explicitly provided)
        for key, value in vars(args).items():
            if value is not None:  # Only override if value was provided on command line
                setattr(config_args, key, value)
        
        # Use the merged arguments
        args = config_args
    
    # Set default values for args not specified either in config or command line
    if not hasattr(args, 'replicates') or args.replicates is None:
        args.replicates = 1
    if not hasattr(args, 'output_dir') or args.output_dir is None:
        args.output_dir = '.'
    if not hasattr(args, 'prefix') or args.prefix is None:
        args.prefix = 'run'
    if not hasattr(args, 'summary_file') or args.summary_file is None:
        args.summary_file = 'dataset_summary.csv'
    if not hasattr(args, 'output_search') or args.output_search is None:
        args.output_search = 'search_set.fasta'
    if not hasattr(args, 'output_background') or args.output_background is None:
        args.output_background = 'background_set.fasta'
    if not hasattr(args, 'background_size') or args.background_size is None:
        args.background_size = 1000
    if not hasattr(args, 'background_mode') or args.background_mode is None:
        args.background_mode = 'select'
    if not hasattr(args, 'shuffle_method') or args.shuffle_method is None:
        args.shuffle_method = 'naive'
    if not hasattr(args, 'dry_run'):
        args.dry_run = False
    
    # Validate required arguments
    if not hasattr(args, 'fasta'):
        parser.error("No FASTA input file specified. Use --fasta or provide it in the config file.")
    
    # Ensure at least one motif option is provided
    if not hasattr(args, 'motif_length') and not hasattr(args, 'motif_string') and not hasattr(args, 'motif_file'):
        parser.error("No motif specification provided. Use one of --motif-length, --motif-string, or --motif-file.")
    
    return args


def determine_run_mode(args):
    """
    Determine whether to run in parameter sweep mode or single dataset mode.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        str: 'sweep' for parameter sweep mode, 'single' for single dataset mode
    """
    # Check if parameter sweep arguments are provided
    if hasattr(args, 'test_sizes') and args.test_sizes and hasattr(args, 'injection_rates') and args.injection_rates:
        print("Running in parameter sweep mode")
        return 'sweep'
    
    # Check if single dataset arguments are provided
    if hasattr(args, 'search_size') and args.search_size and hasattr(args, 'injection_rate') and args.injection_rate:
        print("Running in single dataset mode")
        return 'single'
    
    # Not enough arguments to determine the mode
    return None


def main():
    # Parse command-line arguments
    args = parse_args()
    
    # Print all arguments for verification
    print("MotiFab Dataset Generator")
    print("------------------------")
    print("Parameters:")
    for arg, value in vars(args).items():
        # Skip config file parameter in output
        if arg != 'config':
            print(f"  {arg}: {value}")
    print()
    
    # Determine the run mode
    mode = determine_run_mode(args)
    
    if mode is None:
        print("Error: Insufficient arguments to determine run mode.")
        print("For single dataset mode: --search-size and --injection-rate required")
        print("For parameter sweep mode: --test-sizes and --injection-rates required")
        # Exit with error code 1 instead of returning
        sys.exit(1)
    
    # If dry-run is set, exit here
    if hasattr(args, 'dry_run') and args.dry_run:
        print("Dry run mode: no datasets will be generated")
        return 0
    
    # Build the flow (do this only once)
    print(f"Building flow for {mode} mode...")
    try:
        flow_manager, pipes = build_motif_dataset_flow(args)
    except Exception as e:
        print(f"Error building flow: {e}")
        # Exit with error code 1 instead of returning
        sys.exit(1)
    
    # Execute based on the mode
    if mode == 'single':
        # Create output directory if needed
        output_dir = os.path.dirname(os.path.abspath(args.output_search))
        os.makedirs(output_dir, exist_ok=True)
        
        # Copy PWM file if using one
        if hasattr(args, 'motif_file') and args.motif_file:
            try:
                pwm_filename = copy_pwm_to_output_dir(args.motif_file, output_dir)
                print(f"Copied PWM file to output directory: {pwm_filename}")
            except Exception as e:
                print(f"Error copying PWM file: {e}")
                return 1
        
        # Create parameters for single dataset run
        dataset_params = {
            'fasta_file_path': args.fasta,
            'search_size': args.search_size,
            'injection_rate': args.injection_rate,
            'background_size': args.background_size,
            'output_search_path': args.output_search,
            'output_background_path': args.output_background
        }
        
        # Add motif-specific parameters
        if hasattr(args, 'motif_string') and args.motif_string:
            dataset_params['motif_string'] = args.motif_string
        elif hasattr(args, 'motif_file') and args.motif_file:
            dataset_params['pwm_file_path'] = args.motif_file
            
        # Run the flow
        result = run_dataset_generator(flow_manager, dataset_params)
        
        return 0 if result else 1
    else:  # parameter sweep mode
        success = generate_multiple_datasets(flow_manager, args)
        return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())