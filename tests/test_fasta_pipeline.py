import os
import sys
import pytest
from src.fasta_utils import load_fasta

# Dummy FASTA content that does NOT contain the motif "GGGGGGGG".
FASTA_CONTENT = """>seq1
ACGTACGTACGT
>seq2
TTTTCCCCAAAA
>seq3
ACACACACACAC
>seq4
TATATATATA
>seq5
CGCGCGCGCG
"""

@pytest.fixture
def temp_fasta_file(tmp_path):
    """
    Creates a temporary FASTA file with dummy content and returns its path.
    """
    fasta_path = tmp_path / "input.fasta"
    fasta_path.write_text(FASTA_CONTENT)
    return str(fasta_path)

@pytest.fixture
def output_paths(tmp_path):
    """
    Provides temporary output paths for the search and background FASTA files.
    """
    search_path = tmp_path / "search_set.fasta"
    background_path = tmp_path / "background_set.fasta"
    return str(search_path), str(background_path)

def count_injected(records, motif):
    """
    Counts the number of records in which the sequence contains the specified motif substring.
    """
    count = 0
    for rec in records:
        if motif in rec["seq"]:
            count += 1
    return count

def test_pipeline_select_mode(monkeypatch, tmp_path, temp_fasta_file, output_paths):
    """
    Test the integrated pipeline using background_mode "select".
    - Uses a search set size of 3 records.
    - Uses a provided motif string "GGGGGGGG".
    - Sets injection rate to "50%" (i.e. about half of the search set should be injected).
    - Verifies that the search set output contains approximately 50% injected records,
      and that the background set output has the expected number of records.
    """
    search_size = 3
    # Since our dummy FASTA has 5 records and we select 3 for the search set,
    # the remaining records (if any) will be used for background.
    background_size = 2  
    injection_rate = "50%"  # Expect about 50% of search set get injected.
    provided_motif = "GGGGGGGG"  # Provided motif (should not already be in FASTA).
    output_search, output_background = output_paths

    test_args = [
        "cli.py",
        "--fasta", temp_fasta_file,
        "--motif-string", provided_motif,
        "--search-size", str(search_size),
        "--injection-rate", injection_rate,
        "--background-mode", "select",
        "--output-search", output_search,
        "--output-background", output_background
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    from src.cli import main
    main()

    # Load output FASTA files.
    search_records = load_fasta(output_search)
    background_records = load_fasta(output_background)

    # Verify that the search set has exactly search_size records.
    assert len(search_records) == search_size

    # Count how many records in the search set contain the provided motif.
    injected_count = count_injected(search_records, provided_motif)
    expected_injections = int(round(search_size * 0.5))
    # Allow a difference of at most 1 due to rounding.
    assert abs(injected_count - expected_injections) <= 1

    # In select mode, the background set should have exactly background_size records.
    assert len(background_records) == background_size

    # Also, ensure that none of the background records contain the injected motif.
    for rec in background_records:
        assert provided_motif not in rec["seq"]

def test_pipeline_shuffle_mode(monkeypatch, tmp_path, temp_fasta_file, output_paths):
    """
    Test the integrated pipeline using background_mode "shuffle".
    - Uses a search set size of 3 records.
    - Uses a provided motif string "GGGGGGGG".
    - Sets injection rate as an absolute value: exactly 1 record injected.
    - Uses background_mode "shuffle" with "naive" shuffling.
    - Verifies that the search set output has exactly 1 injected record,
      and that the background set output has exactly the specified background_size records.
    """
    search_size = 3
    background_size = 4
    injection_rate = "1"  # Absolute: exactly 1 record gets injected.
    provided_motif = "GGGGGGGG"
    output_search, output_background = output_paths

    test_args = [
        "cli.py",
        "--fasta", temp_fasta_file,
        "--motif-string", provided_motif,
        "--search-size", str(search_size),
        "--injection-rate", injection_rate,
        "--background-mode", "shuffle",
        "--background-size", str(background_size),
        "--shuffle-method", "naive",
        "--output-search", output_search,
        "--output-background", output_background
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    from src.cli import main
    main()

    # Load output FASTA files.
    search_records = load_fasta(output_search)
    background_records = load_fasta(output_background)

    # Check that the search set contains exactly search_size records.
    assert len(search_records) == search_size

    # Count the number of records with the injected motif.
    injected_count = count_injected(search_records, provided_motif)
    assert injected_count == 1

    # In shuffle mode, the background set should have exactly background_size records.
    assert len(background_records) == background_size

    # Ensure that each background record contains only valid nucleotides.
    for rec in background_records:
        for ch in rec["seq"]:
            assert ch in "ACGT"