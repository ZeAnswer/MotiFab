import os
import sys
import csv
import pytest
import tempfile
import subprocess
from pathlib import Path
from src.motif_dataset_generator import main as generator_main

@pytest.fixture
def dummy_fasta(tmp_path):
    """
    Creates a temporary FASTA file with sample sequences for testing.
    """
    content = """>seq1
ACGTACGTACGT
>seq2
TTTTCCCCAAAA
>seq3
ACACACACACAC
>seq4
TATATATATA
>seq5
CGCGCGCGCGCG
"""
    fasta_file = tmp_path / "dummy.fasta"
    fasta_file.write_text(content)
    return str(fasta_file)

@pytest.fixture
def temp_output_dir(tmp_path):
    """
    Creates a temporary directory for output files.
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir

def test_single_dataset_generation(monkeypatch, dummy_fasta, tmp_path, capsys):
    """
    Test that a single dataset can be generated with expected parameters.
    """
    # Setup output paths
    output_search = tmp_path / "test_search.fasta"
    output_background = tmp_path / "test_background.fasta"
    
    # Build the command-line arguments
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "AAACCCTTTGGG",
        "--search-size", "3",
        "--injection-rate", "2",
        "--background-mode", "select",
        "--output-search", str(output_search),
        "--output-background", str(output_background),
        "--background-size", "2"
    ]
    
    # Mock sys.argv
    monkeypatch.setattr(sys, "argv", args)
    
    # Run the generator
    generator_main()
    
    # Check that output files exist
    assert output_search.exists(), "Search FASTA file was not created"
    assert output_background.exists(), "Background FASTA file was not created"
    
    # Check file content (basic validation)
    search_content = output_search.read_text()
    bg_content = output_background.read_text()
    
    assert ">seq" in search_content, "Search file should contain sequence headers"
    assert ">seq" in bg_content, "Background file should contain sequence headers"

def test_parameter_sweep_mode(monkeypatch, dummy_fasta, temp_output_dir, capsys):
    """
    Test parameter sweep mode with multiple test sizes and injection rates.
    """
    # Test summary file
    summary_file = "summary.csv"
    summary_path = temp_output_dir / summary_file

    # Build the command-line arguments
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--test-sizes", "2,3",
        "--injection-rates", "1,50%",
        "--motif-string", "AAACCCTTTGGG",
        "--background-size", "2",
        "--background-mode", "shuffle",
        "--shuffle-method", "di-pair",
        "--output-dir", str(temp_output_dir),
        "--prefix", "test",
        "--replicates", "1",
        "--summary-file", summary_file
    ]
    
    # Mock sys.argv
    monkeypatch.setattr(sys, "argv", args)
    
    # Run the generator
    generator_main()
    
    # Check that the summary file exists
    assert summary_path.exists(), "Summary CSV file was not created"
    
    # Read the summary CSV and check contents
    with open(summary_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        
        # Debug the rows to check what's wrong
        print(f"Found rows: {rows}")
        
        assert len(rows) == 4, f"Expected four parameter combinations, got {len(rows)}"
        
        # Check that expected combinations are present
        combinations = [(row["test_size"], row["injection_rate"]) for row in rows]
        expected_combinations = [('2', '1'), ('2', '50%'), ('3', '1'), ('3', '50%')]
        
        # Use sets for easier comparison, ignoring order
        assert set(map(tuple, combinations)) == set(map(tuple, expected_combinations)), f"Missing some parameter combinations. Found: {combinations}"
        
        # Check that output files exist and have expected naming patterns
        for row in rows:
            search_file = temp_output_dir / row["output_search"]
            bg_file = temp_output_dir / row["output_background"]
            
            assert search_file.exists(), f"Search file {row['output_search']} not created"
            assert bg_file.exists(), f"Background file {row['output_background']} not created"

def test_error_handling(monkeypatch, dummy_fasta, capsys):
    """
    Test error handling for invalid parameter combinations.
    """
    # Case 1: No mode specified (neither single nor parameter sweep)
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "AAACCCTTTGGG",
        # Missing both --search-size/--injection-rate and --test-sizes/--injection-rates
    ]
    
    monkeypatch.setattr(sys, "argv", args)
    
    # Should exit with error
    with pytest.raises(SystemExit):
        generator_main()
    
    # Case 2: Invalid motif characters
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "AAAXXXGGG",  # X is not valid
        "--search-size", "3",
        "--injection-rate", "2"
    ]
    
    monkeypatch.setattr(sys, "argv", args)
    
    # Should fail during execution
    result = generator_main()
    assert result != 0, "Should not succeed with invalid motif"

def test_dry_run_mode(monkeypatch, dummy_fasta, capsys):
    """
    Test that dry-run mode parses arguments but doesn't generate files.
    """
    output_search = "should_not_be_created.fasta"
    output_background = "bg_should_not_be_created.fasta"
    
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "AAACCCTTTGGG",
        "--search-size", "3",
        "--injection-rate", "2",
        "--output-search", output_search,
        "--output-background", output_background,
        "--dry-run"
    ]
    
    monkeypatch.setattr(sys, "argv", args)
    
    # Run in dry-run mode
    generator_main()
    
    # Check output
    captured = capsys.readouterr()
    assert "Dry run mode" in captured.out
    
    # Files should not exist
    assert not os.path.exists(output_search)
    assert not os.path.exists(output_background)

def test_motif_specification_options(monkeypatch, dummy_fasta, tmp_path):
    """
    Test the different ways to specify a motif.
    """
    output_dir = tmp_path / "motif_tests"
    output_dir.mkdir()
    
    # Test 1: Random motif of specified length
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-length", "8",
        "--search-size", "3",
        "--injection-rate", "1",
        "--output-search", str(output_dir / "random_motif_search.fasta"),
        "--output-background", str(output_dir / "random_motif_bg.fasta")
    ]
    
    monkeypatch.setattr(sys, "argv", args)
    generator_main()
    
    assert (output_dir / "random_motif_search.fasta").exists()
    
    # Test 2: Motif string
    args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "ACGTACGT",
        "--search-size", "3",
        "--injection-rate", "1",
        "--output-search", str(output_dir / "string_motif_search.fasta"),
        "--output-background", str(output_dir / "string_motif_bg.fasta")
    ]
    
    monkeypatch.setattr(sys, "argv", args)
    generator_main()
    
    assert (output_dir / "string_motif_search.fasta").exists()

# Additional CLI tests ported from test_cli.py

def test_cli_with_motif_string(monkeypatch, capsys):
    """
    Test that the CLI correctly parses parameters when a motif string is provided.
    Uses --dry-run so no actual file I/O is attempted.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--search-size", "150",
        "--injection-rate", "10%",
        "--background-mode", "select",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    generator_main()
    
    # Capture the output
    captured = capsys.readouterr()
    
    # Verify that expected parameters are printed
    assert "Parsed parameters" in captured.out or "Parameters:" in captured.out
    assert "dummy.fasta" in captured.out
    assert "ACGTACGT" in captured.out
    assert "150" in captured.out
    assert "10%" in captured.out
    assert "select" in captured.out
    assert "Dry run mode" in captured.out

def test_cli_with_motif_length(monkeypatch, capsys):
    """
    Test that the CLI correctly parses parameters when a motif length is provided.
    Uses --dry-run so no actual file I/O is attempted.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-length", "12",
        "--search-size", "150",
        "--injection-rate", "15",      # Using absolute number
        "--background-mode", "shuffle",
        "--shuffle-method", "di-pair",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    generator_main()
    
    # Capture the output
    captured = capsys.readouterr()
    
    # Verify that expected parameters are printed
    assert "Parameters:" in captured.out
    assert "dummy.fasta" in captured.out
    assert "12" in captured.out
    assert "150" in captured.out
    assert "15" in captured.out
    assert "shuffle" in captured.out
    assert "di-pair" in captured.out
    assert "Dry run mode" in captured.out

def test_cli_default_values(monkeypatch, capsys):
    """
    Test that default values are used correctly when not specified.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGT",
        "--search-size", "50",
        "--injection-rate", "5",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    generator_main()
    
    # Capture the output
    captured = capsys.readouterr()
    
    # Check for default values in output
    assert "background_mode" in captured.out and "select" in captured.out
    assert "background_size" in captured.out and "1000" in captured.out

def test_cli_error_multiple_motif_options(monkeypatch, capsys):
    """
    Test that an error is raised when multiple motif options are specified.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGT",
        "--motif-length", "8",         # Both string and length specified
        "--search-size", "50",
        "--injection-rate", "5"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    
    # Should exit with error
    with pytest.raises(SystemExit):
        generator_main()

def test_cli_error_no_motif_option(monkeypatch, capsys):
    """
    Test that an error is raised when no motif option is specified.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--search-size", "50",
        "--injection-rate", "5"
        # Missing motif option
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    
    # Should exit with error
    with pytest.raises(SystemExit):
        generator_main()

def test_cli_error_missing_search_size(monkeypatch, capsys):
    """
    Test that an error is noted when in single dataset mode and missing search size.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGT",
        "--injection-rate", "5"
        # Missing search size
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    
    # Should exit with error code 1
    with pytest.raises(SystemExit):
        generator_main()
    
    # Capture and check output
    captured = capsys.readouterr()
    assert "Insufficient arguments" in captured.out

def test_cli_error_missing_injection_rate(monkeypatch, capsys):
    """
    Test that an error is noted when in single dataset mode and missing injection rate.
    """
    test_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGT",
        "--search-size", "50"
        # Missing injection rate
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    
    # Should exit with error code 1
    with pytest.raises(SystemExit):
        generator_main()
    
    # Capture and check output
    captured = capsys.readouterr()
    assert "Insufficient arguments" in captured.out

def test_cli_single_vs_sweep_mode(monkeypatch, capsys):
    """
    Test that the CLI correctly determines the mode based on argument patterns.
    """
    # Test single mode
    single_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGT",
        "--search-size", "50",
        "--injection-rate", "5",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", single_args)
    generator_main()
    captured = capsys.readouterr()
    assert "Running in single dataset mode" in captured.out
    
    # Test sweep mode
    sweep_args = [
        "motif_dataset_generator.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGT",
        "--test-sizes", "40,50,60",
        "--injection-rates", "5%,10%",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", sweep_args)
    generator_main()
    captured = capsys.readouterr()
    assert "Running in parameter sweep mode" in captured.out

def test_shuffle_methods(monkeypatch, dummy_fasta, tmp_path):
    """
    Test that different shuffle methods work correctly.
    """
    output_dir = tmp_path / "shuffle_tests"
    output_dir.mkdir()
    
    # Test with naive shuffle
    naive_args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "ACGTACGT",
        "--search-size", "3",
        "--injection-rate", "1",
        "--background-mode", "shuffle",
        "--shuffle-method", "naive",
        "--output-search", str(output_dir / "naive_search.fasta"),
        "--output-background", str(output_dir / "naive_background.fasta")
    ]
    
    monkeypatch.setattr(sys, "argv", naive_args)
    generator_main()
    
    # Test with di-pair shuffle
    dipair_args = [
        "motif_dataset_generator.py",
        "--fasta", dummy_fasta,
        "--motif-string", "ACGTACGT",
        "--search-size", "3",
        "--injection-rate", "1",
        "--background-mode", "shuffle",
        "--shuffle-method", "di-pair",
        "--output-search", str(output_dir / "dipair_search.fasta"),
        "--output-background", str(output_dir / "dipair_background.fasta")
    ]
    
    monkeypatch.setattr(sys, "argv", dipair_args)
    generator_main()
    
    # Check that all files were created
    assert (output_dir / "naive_search.fasta").exists()
    assert (output_dir / "naive_background.fasta").exists()
    assert (output_dir / "dipair_search.fasta").exists() 
    assert (output_dir / "dipair_background.fasta").exists()