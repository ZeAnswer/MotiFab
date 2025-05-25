#!/usr/bin/env python3
"""
Script to enrich motif summary results with information about detected motifs.
Processes meme.xml or homer.txt files to extract motif information and appends it to summary data.
"""
import os
import sys
import csv
import argparse
from pathlib import Path
import pandas as pd
from pandas.errors import ParserError
import xml.etree.ElementTree as ET

# Add parent directory to path to allow importing from project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flowline.flow_pipes.motif_detection_pipes import (
    MemeXmlParserPipe, HomerTextParserPipe, MotifLocalAlignmentPipe, StringToOneShotPWMPipe, 
    PWMComparisonPipe, MotifSummaryPipe
)


def parse_motif_results(output_dir, run_id, tool_type='meme'):
    """Parse motif results file to extract motif data."""
    # Create the appropriate parser pipe
    if tool_type == 'meme':
        parser_pipe = MemeXmlParserPipe()
    else:  # homer
        parser_pipe = HomerTextParserPipe()
    
    # Execute the pipe
    try:
        result = parser_pipe.execute({
            'output_dir': output_dir,
            'status': 'COMPLETED'  # Assume the job completed successfully
        })
        
        # Return the motifs
        return result.get('motifs', [])
    except Exception as e:
        print(f"Error parsing {tool_type.upper()} results for run {run_id}: {e}")
        return []


def find_motif_matches(motifs, injected_motif, similarity_threshold=0.7):
    """Find motif matches using local alignment or PWM comparison."""
    # Use PWM comparison if the injected motif is a valid PWM file
    if os.path.isfile(injected_motif):
        # Process PWM file
        # This is a placeholder, actual implementation would depend on the PWM format
        print(f"PWM comparison not implemented for file: {injected_motif}")
        return []
    
    # Try local alignment for string motifs
    try:
        # Create the motif alignment pipe
        aligner_pipe = MotifLocalAlignmentPipe(
            injected_motif=injected_motif,
            similarity_threshold=similarity_threshold
        )
        
        # Execute the pipe
        alignment_result = aligner_pipe.execute({
            'discovered_motifs': motifs
        })
        
        return alignment_result.get('matched_motifs', [])
    except Exception as e:
        print(f"Error finding motif matches: {e}")
        return []


def process_run_data(
    summary_df, summary_row_index, output_base_dir, 
    injected_motif, tool_type='meme', similarity_threshold=0.7
):
    """
    Process a single run's data from summary and update the dataframe.
    
    Args:
        summary_df: DataFrame containing summary data
        summary_row_index: Index of the row to process
        output_base_dir: Base directory containing output folders
        injected_motif: Injected motif string to search for
        tool_type: Type of tool being used ('meme' or 'homer')
        similarity_threshold: Similarity threshold for match detection
    
    Returns:
        Updated summary DataFrame
    """
    # Get the run_id from the summary row
    run_id = summary_df.loc[summary_row_index, 'run_id']
    
    # Create path to the output directory
    output_dir = os.path.join(output_base_dir, str(run_id))
    
    # Check if the directory exists
    if not os.path.isdir(output_dir):
        print(f"Output directory not found for run {run_id}: {output_dir}")
        return summary_df
    
    # Parse motif results from the output directory
    motifs = parse_motif_results(output_dir, run_id, tool_type)
    
    if not motifs:
        print(f"No motifs found for run {run_id}")
        return summary_df
    
    # Find motif matches
    matched_motifs = find_motif_matches(
        motifs, 
        injected_motif, 
        similarity_threshold
    )
    
    # Create summary record from the row
    summary_record = {
        'run_id': run_id,
        'test_size': summary_df.loc[summary_row_index, 'test_size'],
        'injection_rate': summary_df.loc[summary_row_index, 'injection_rate'],
        'motif': summary_df.loc[summary_row_index, 'motif']
    }
    
    # Process motif summary with the matched motifs
    summary_pipe = MotifSummaryPipe()
    try:
        summary_result = summary_pipe.execute({
            'matched_motifs': matched_motifs,
            'summary_record': summary_record
        })
        
        enriched_record = summary_result.get('enriched_record', {})
        
        # Update the summary dataframe with the new data
        if enriched_record:
            # Update basic match information
            summary_df.loc[summary_row_index, 'is_match'] = enriched_record.get('is_match', False)
            summary_df.loc[summary_row_index, 'similarity_score'] = enriched_record.get('similarity_score', 0.0)
            
            # Update additional match information if present
            alignment = enriched_record.get('alignment', {})
            if alignment:
                if isinstance(alignment, dict) and 'injected' in alignment and 'discovered' in alignment:
                    summary_df.loc[summary_row_index, 'alignment_injected'] = alignment.get('injected', '')
                    summary_df.loc[summary_row_index, 'alignment_discovered'] = alignment.get('discovered', '')
                else:
                    # For PWM-based comparisons, alignment might just be an offset
                    summary_df.loc[summary_row_index, 'alignment_offset'] = alignment
            
            # Add matched consensus if it exists
            summary_df.loc[summary_row_index, 'matched_consensus'] = enriched_record.get('matched_consensus', '')
            
    except Exception as e:
        print(f"Error processing summary for run {run_id}: {e}")
    
    return summary_df


def main(summary_file, output_base_dir, injected_motif, output_file, tool_type='meme', similarity_threshold=0.7):
    """
    Main function to process summary data and enrich it with motif information.
    
    Args:
        summary_file: Path to the summary CSV file
        output_base_dir: Base directory containing output folders
        injected_motif: Injected motif string to search for
        output_file: Path to write the enriched summary CSV file
        tool_type: Type of tool being used ('meme' or 'homer')
        similarity_threshold: Similarity threshold for match detection
    """
    try:
        # Read the summary CSV file
        summary_df = pd.read_csv(summary_file)
        
        # Process each row in the summary
        for index, row in summary_df.iterrows():
            summary_df = process_run_data(
                summary_df, index, output_base_dir, 
                injected_motif, tool_type, similarity_threshold
            )
        
        # Write the enriched summary to the output file
        summary_df.to_csv(output_file, index=False)
        print(f"Enriched summary written to {output_file}")
        
    except ParserError as e:
        print(f"Error parsing summary file: {e}")
    except Exception as e:
        print(f"Error processing summary: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich motif summary with detected motifs")
    parser.add_argument("--summary-file", required=True, help="Path to the summary CSV file")
    parser.add_argument("--output-dir", required=True, help="Base directory containing output folders")
    parser.add_argument("--injected-motif", required=True, help="Injected motif string to search for")
    parser.add_argument("--output-file", required=True, help="Path to write the enriched summary CSV file")
    parser.add_argument("--tool-type", choices=['meme', 'homer'], default='meme', help="Type of tool being used")
    parser.add_argument("--similarity-threshold", type=float, default=0.7, help="Similarity threshold for match detection")
    
    args = parser.parse_args()
    
    main(
        args.summary_file, 
        args.output_dir, 
        args.injected_motif, 
        args.output_file, 
        args.tool_type, 
        args.similarity_threshold
    )