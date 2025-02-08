import os
import csv
import sys
import pytest
import subprocess
from src.dataset_generator_wrapper import main as dg_main
#TODO more tests for this. these tests are very lacking and are due to a lack of time. need to test more parameters and more edge cases.
@pytest.fixture
def dummy_fasta(tmp_path):
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

def test_dataset_generator_wrapper(tmp_path, monkeypatch, dummy_fasta, capsys):
    """
    A basic integration test for the dataset generator wrapper.
    This test runs the wrapper with a single combination (one test size, one injection rate, one replicate)
    and then checks that the summary CSV file is created and contains the expected row.
    """
    output_dir = tmp_path / "datasets"
    output_dir.mkdir()
    summary_file = "summary.csv"

    # Build the command-line arguments for the wrapper.
    args = [
        "dataset_generator_wrapper.py",
        "--input-fasta", dummy_fasta,
        "--test-sizes", "40",
        "--injection-rates", "10%",
        "--motif-string", "AAACCCTTTGGG",
        "--background-size", "1000",
        "--background-mode", "shuffle",
        "--shuffle-method", "di-pair",
        "--output-dir", str(output_dir),
        "--prefix", "run",
        "--num-runs", "1",
        "--summary-file", summary_file
    ]
    monkeypatch.setattr(sys, "argv", args)
    # Run the wrapper.
    dg_main()
    # Check that the summary file exists.
    summary_path = output_dir / summary_file
    assert summary_path.exists(), "Summary CSV file was not created."
    # Read the summary CSV and check contents.
    with open(summary_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        assert len(rows) == 1, "Expected exactly one run in the summary."
        row = rows[0]
        assert row["test_size"] == "40"
        assert row["injection_rate"] == "10%"
        assert row["motif"] == "AAACCCTTTGGG"
        # Check that output filenames include the expected prefixes.
        assert row["output_search"].startswith("run_test_40_10pct")
        assert row["output_background"].startswith("run_background_40_10pct")