# Motif Enrichment Analysis

This tool performs motif enrichment analysis on RNA sequences using either MEME or HOMER tools.

## Prerequisites

- Python 3.6+
- MEME Suite (if using MEME)
- HOMER (if using HOMER)
- Required Python packages:
  - pandas
  - flowline

## Configuration

The script uses a configuration file (`enrichment_config.ini`) to specify parameters for the analysis. The configuration file has the following sections:

### Default Section
- `summary_file`: Path to the CSV file containing dataset information
- `output_dir`: Directory where results will be stored
- `tool`: Tool to use for motif analysis (meme or homer)
- `wait_for_completion`: Whether to wait for job completion
- `poll_interval`: Interval (in seconds) to check job status

### General Section
- `motif_length`: Comma-separated list of motif lengths to search for
- `num_motifs`: Number of motifs to find
- `strand`: Strand to search (both, plus, minus)
- `revcomp`: Whether to search reverse complement

### MEME Section
- `module_name`: Name of the MEME module to load
- `extra_params`: Additional MEME parameters

### HOMER Section
- `module_name`: Name of the HOMER module to load
- `extra_params`: Additional HOMER parameters

## Usage

```bash
python src/motif_enrichment.py [options]
```

### Options:
- `-c, --config`: Path to configuration file (default: enrichment_config.ini)
- `-s, --summary`: Path to summary CSV file (overrides config file)
- `-o, --output`: Output directory (overrides config file)
- `-t, --tool`: Tool to use (meme or homer) (overrides config file)

## Input Format

The summary CSV file should contain the following columns:
- `dataset_id`: Unique identifier for the dataset
- `fasta_file`: Path to the FASTA file containing sequences
- `background_file`: Path to the background FASTA file (optional)

## Output

For each dataset, the script creates a directory named after the dataset ID in the output directory. The directory contains:
- `motifs.txt`: Found motifs
- `motif_logo.png`: Logo plot of the motifs
- `output.log`: Analysis log file

## Example

```bash
# Using default configuration
python src/motif_enrichment.py

# Using custom configuration
python src/motif_enrichment.py -c my_config.ini

# Overriding configuration options
python src/motif_enrichment.py -s my_summary.csv -o ./my_results -t homer
```

