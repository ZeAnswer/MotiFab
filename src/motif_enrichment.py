#!/usr/bin/env python3
"""
MotiFab: Motif Enrichment Analysis Tool

This tool runs motif enrichment analysis on FASTA datasets using MEME and other tools.
It can operate in batch mode across multiple datasets and provides a summary of results.

Features:
  - Run MEME enrichment analysis across multiple datasets
  - Customize MEME parameters for analysis
  - Export summary information for further analysis
  - Support for configuration via INI files

Batch Mode Usage:
  python src/motif_enrichment.py --dataset-summary summary.csv --output-dir ./results

Config File Usage:
  python src/motif_enrichment.py --config my_config.ini
  (Command line arguments override config file settings)
"""

import os
import sys
import csv
import argparse
import configparser
import logging
from pathlib import Path

# given a csv file path, read the file and return an array of dictionaries
def read_csv(file_path):
    with open(file_path, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = []
        for row in reader:
            data.append(row)
    return data 

# given a csv file path and an array of dictionaries, write the array to the csv file
def write_csv(file_path, data):
    with open(file_path, mode='w', newline='') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

# Import flow modules
from flowline import (
    FlowManager, FlowPipe, FlowOutputFilter,
    MemeCommandGeneratorPipe,
    build_flow, JobExecutorPipe, MemeXmlParserPipe, MotifLocalAlignmentPipe, PWMComparisonPipe, ParsePWMPipe, CommandExecutorPipe, MotifSummaryPipe, FlowSubPipeline, FlowSplitJoinPipe#, HomerTextParserPipe, HomerCommandGeneratorPipe
)

# --- Build the sub-flow configuration ---
def build_sub_flow(files_dir, output_dir, tool_type='meme', config=None, injected_motif=None):
    """
    Build a sub-flow that processes a single summary_record and produces an enriched_record.
    
    Args:
        files_dir (str): Directory where the input FASTA files are stored.
        output_dir (str): Directory where results will be stored.
        tool_type (str): Type of tool being used ('meme' or 'homer').
        config (configparser.ConfigParser): Configuration object with parameters.
        injected_motif (str): Motif string to use for local alignment or path to PWM file.
        
    Returns:
        tuple: (FlowManager, dict) for the sub-flow.
    """
    
    # Check if injected_motif is a PWM file
    use_pwm = False
    if injected_motif and os.path.isfile(injected_motif):
        use_pwm = True
    
    # Get common parameters from the general section
    general_section = {}
    if config and config.has_section('general'):
        general_section = dict(config['general'])
    
    motif_length = general_section.get('motif_length')
    num_motifs = general_section.get('num_motifs')
    strand = general_section.get('strand')
    revcomp = general_section.get('revcomp')
    
    # Get tool-specific parameters
    tool_section = {}
    if config and config.has_section(tool_type):
        tool_section = dict(config[tool_type])
    
    extra_params = tool_section.get('extra_params')
    
    sub_flow_config = {}
    
    # Pipe 1: Generate command based on tool type
    if tool_type == 'meme':
        sub_flow_config['command_gen'] = {
            'type': MemeCommandGeneratorPipe,
            'init': {
                'output_dir_prefix': output_dir,
                'files_dir': files_dir,
                'extra_params': extra_params,
                'motif_length': motif_length,
                'num_motifs': num_motifs,
                'strand': strand,
                'revcomp': revcomp
            },
            'upstream_pipes': {
                '*': {'summary_record': 'summary_record'}
            }
        }
    # else:  # homer
    #     sub_flow_config['command_gen'] = {
    #         'type': HomerCommandGeneratorPipe,
    #         'init': {
    #             'output_dir_prefix': output_dir,
    #             'files_dir': files_dir,
    #             'extra_params': extra_params,
    #             'motif_length': motif_length,
    #             'num_motifs': num_motifs,
    #             'strand': strand,
    #             'revcomp': revcomp
    #         },
    #         'upstream_pipes': {
    #             '*': {'summary_record': 'summary_record'}
    #         }
    #     }
    
    # Pipe 2: Execute command.
    sub_flow_config['command_exec'] = {
        'type': CommandExecutorPipe,
        'init': {},
        'upstream_pipes': {
            'command_gen': {'command': 'command', 'output_dir': 'output_dir'}
        }
    }
    
    # Pipe 3: Parse results based on tool type
    if tool_type == 'meme':
        sub_flow_config['result_parse'] = {
            'type': MemeXmlParserPipe,
            'init': {},
            'upstream_pipes': {
                'command_exec': {'status': 'status'},
                'command_gen': {'output_dir': 'output_dir'}
            }
        }
    # else:  # homer
    #     sub_flow_config['result_parse'] = {
    #         'type': HomerTextParserPipe,
    #         'init': {},
    #         'upstream_pipes': {
    #             'command_exec': {'status': 'status'},
    #             'command_gen': {'output_dir': 'output_dir'}
    #         }
    #     }
    
    if use_pwm:
        # PWM branch.
        sub_flow_config['parse_pwm'] = {
            'type': ParsePWMPipe,
            'init': {'pwm_file_path': injected_motif},
            'upstream_pipes': {
            }
        }
        sub_flow_config['motif_compare'] = {
            'type': PWMComparisonPipe,
            'init': {
            },
            'upstream_pipes': {
                'result_parse': {'motifs': 'discovered_motifs'},
                'parse_pwm': {'pwm_matrix': 'injected_pwm'}
            }
        }
    else:
        # Local alignment branch.
        sub_flow_config['motif_compare'] = {
            'type': MotifLocalAlignmentPipe,
            'init': {
                'injected_motif': injected_motif
            },
            'upstream_pipes': {
                'result_parse': {'motifs': 'discovered_motifs'}
            }
        }
    
    # Final pipe: Enrich summary.
    sub_flow_config['summary_enrich'] = {
        'type': MotifSummaryPipe,
        'init': {},
        'upstream_pipes': {
            '*': {'summary_record': 'summary_record'},
            'motif_compare': {'matched_motifs': 'matched_motifs'}
        }
    }
    
    # Build the sub-flow.
    sub_manager, sub_pipes = build_flow(sub_flow_config)
    return sub_manager, sub_pipes

# --- Build the overall flow using FlowSplitJoinPipe ---
def build_overall_flow(files_dir, output_dir, injected_motif, tool_type='meme', config=None):
    """
    Build the overall flow that processes multiple summary records in parallel.
    
    Args:
        files_dir (str): Directory where the input FASTA files are stored.
        output_dir (str): Directory where results will be stored.
        injected_motif (str): Motif string to use for local alignment or path to PWM file.
        tool_type (str): Type of tool being used ('meme' or 'homer').
        config (configparser.ConfigParser): Configuration object with parameters.
        
    Returns:
        FlowSplitJoinPipe instance.
    """
    sub_manager, sub_pipes = build_sub_flow(files_dir, output_dir, tool_type, config, injected_motif)
    
    enrichment_flow = {}
        
    subPipeline = FlowSubPipeline(sub_manager, inputs=['summary_record'], outputs=['enriched_record'])
    # Wrap the sub-pipeline in a FlowSplitJoinPipe to process multiple records concurrently.
    enrichment_flow['split_join'] = {
        'type': FlowSplitJoinPipe,
        'init': {
            'inner_pipe': subPipeline,
            'input_mapping': {'summary_record': 'i'},
            'max_parallel': 70
        },
        'upstream_pipes': {
            '*': {'summary_record': 'summary_record'}
        }
    }
    
    # Build the overall flow.
    manager, pipes = build_flow(enrichment_flow)
    return manager


# --- Main execution ---
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Motif Enrichment Analysis Tool')
    parser.add_argument('-c', '--config', default='enrichment_config.ini', help='Path to configuration file')
    parser.add_argument('-s', '--summary', help='Path to summary CSV file (overrides config file)')
    parser.add_argument('-f', '--files-dir', help='Path to files directory (overrides config file)')
    parser.add_argument('-o', '--output', help='Output directory (overrides config file)')
    parser.add_argument('-t', '--tool', choices=['meme', 'homer'], help='Tool to use (overrides config file)')
    args = parser.parse_args()
    
    # Load configuration
    config = configparser.ConfigParser()
    config.read(args.config)
    
    # Get default settings
    default_section = {}
    if config.has_section('default'):
        default_section = dict(config['default'])
    
    summary_file = args.summary or default_section.get('summary_file')
    output_dir = args.output or default_section.get('output_dir')
    tool_type = args.tool or default_section.get('tool', 'meme')
    
    # Determine files_dir based on summary file location if not provided
    files_dir = args.files_dir or default_section.get('files_dir')
    if not files_dir and summary_file:
        # assume same directory as summary file
        files_dir = os.path.dirname(os.path.abspath(summary_file))
    elif not files_dir:
        # shouldn't happen because a summary file is required
        print("Error: summary file is required")
        return 1
    
    # Set default output_dir if not provided
    if not output_dir:
        output_dir = os.path.join(files_dir, 'results')
    
    # Read the summary CSV file (each row is a summary_record dictionary)
    try:
        summary_records = read_csv(summary_file)
    except Exception as e:
        print(f"Error reading summary CSV: {e}")
        return 1
    
    # Check if we have a motif to inject
    injected_motif = None
    if summary_records and 'motif' in summary_records[0]:
        injected_motif = summary_records[0].get("motif")
    
    # Change the records property names from output_search,output_background to test_fasta_path, background_fasta_path
    for record in summary_records:
        if 'output_search' in record:
            record["test_fasta_path"] = record.pop("output_search")
        if 'output_background' in record:
            record["background_fasta_path"] = record.pop("output_background")
    
    # Build the overall flow
    overall_flow = build_overall_flow(files_dir, output_dir, injected_motif, tool_type, config)

    # --- Add Logging Here ---
    print(f"--- Starting overall flow for {len(summary_records)} records ---")
    for i, record in enumerate(summary_records):
        print(f"Input record {i}: run_id = {record.get('run_id', 'N/A')}")
    print("--- Executing overall_flow.run() ---")
    # --- End Logging ---

    # Execute the overall flow with the summary records.
    # The input dictionary uses key "summary_record" with a list of records.
    try:
        result = overall_flow.run({'summary_record': summary_records})
    except Exception as e:
        print(f"Error executing overall flow: {e}")
        return 1
    
    # Retrieve the enriched records.
    enriched_records = result.get("enriched_record")
    if enriched_records is None:
        # Alternatively, check the aggregated output.
        enriched_records = result.get("arr_output", [])
    
    # Write the enriched summary CSV.
    enriched_csv_path = os.path.join(output_dir, 'enriched_summary.csv')
    try:
        write_csv(enriched_csv_path, enriched_records)
        print(f"Enriched summary written to: {enriched_csv_path}")
    except Exception as e:
        print(f"Error writing enriched summary CSV: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())