import os
import pytest
from src.fasta_utils import (load_fasta, write_fasta, select_random_sequences,
                             generate_random_fasta_records, generate_random_fasta_file)

# Sample FASTA content for existing file tests.
FASTA_CONTENT = """>seq1 Description for sequence 1
ACGTACGTACGT
ACGTACGT
>seq2
TTTTGGGGCCCC
>seq3 Example sequence 3
AAAAACCCCCGGGGTTTT
"""

@pytest.fixture
def fasta_file(tmp_path):
    """
    Creates a temporary FASTA file with sample content and returns its path.
    """
    fasta_path = tmp_path / "test.fasta"
    fasta_path.write_text(FASTA_CONTENT)
    return str(fasta_path)

def test_load_fasta(fasta_file):
    """
    Tests that the FASTA file is loaded correctly.
    """
    records = load_fasta(fasta_file)
    assert len(records) == 3

    # Verify first record.
    rec1 = records[0]
    assert rec1["id"] == "seq1"
    expected_seq1 = "ACGTACGTACGTACGTACGT"
    assert rec1["seq"] == expected_seq1

    # Verify second record.
    rec2 = records[1]
    assert rec2["id"] == "seq2"
    expected_seq2 = "TTTTGGGGCCCC"
    assert rec2["seq"] == expected_seq2

    # Verify third record.
    rec3 = records[2]
    assert rec3["id"] == "seq3"
    expected_seq3 = "AAAAACCCCCGGGGTTTT"
    assert rec3["seq"] == expected_seq3

def test_write_fasta(tmp_path):
    """
    Tests that FASTA records are written correctly to a file.
    """
    records = [
        {"id": "seq1", "desc": "seq1 Description for sequence 1", "seq": "ACGTACGTACGTACGTACGT"},
        {"id": "seq2", "desc": "seq2", "seq": "TTTTGGGGCCCC"},
        {"id": "seq3", "desc": "seq3 Example sequence 3", "seq": "AAAAACCCCCGGGGTTTT"}
    ]
    output_path = tmp_path / "output.fasta"
    write_fasta(records, str(output_path))
    content = output_path.read_text()
    lines = content.strip().splitlines()
    headers = [line for line in lines if line.startswith(">")]
    assert len(headers) == 3
    assert headers[0] == ">seq1 Description for sequence 1"

def test_select_random_sequences(fasta_file):
    """
    Tests that selecting a random subset of records works correctly.
    """
    records = load_fasta(fasta_file)
    sample = select_random_sequences(records, 2)
    assert len(sample) == 2
    original_ids = {rec["id"] for rec in records}
    for rec in sample:
        assert rec["id"] in original_ids

def test_select_random_sequences_too_many(fasta_file):
    """
    Tests that selecting more records than available raises an error.
    """
    records = load_fasta(fasta_file)
    with pytest.raises(ValueError):
        select_random_sequences(records, len(records) + 1)

# -------------------------
# Tests for Random FASTA Generation
# -------------------------

def test_generate_random_fasta_records():
    """
    Tests that generated random FASTA records meet the specified parameters.
    """
    num_sequences = 5
    min_length = 20
    max_length = 30
    prefix = "testseq"
    
    records = generate_random_fasta_records(num_sequences, min_length, max_length, prefix)
    assert len(records) == num_sequences
    for i, record in enumerate(records):
        # Check identifier.
        assert record["id"] == f"{prefix}{i+1}"
        # Check description.
        assert record["desc"] == f"{prefix}{i+1}"
        # Check sequence length.
        seq_len = len(record["seq"])
        assert min_length <= seq_len <= max_length
        # Check that sequence only contains valid nucleotides.
        for ch in record["seq"]:
            assert ch in "ACGT"

def test_generate_random_fasta_file(tmp_path):
    """
    Tests that a random FASTA file is generated correctly and can be re-loaded.
    """
    output_path = tmp_path / "random.fasta"
    num_sequences = 7
    min_length = 10
    max_length = 15
    prefix = "rand"
    
    # Generate the FASTA file.
    generate_random_fasta_file(str(output_path), num_sequences, min_length, max_length, prefix)
    # Load the generated file.
    records = load_fasta(str(output_path))
    assert len(records) == num_sequences
    for i, record in enumerate(records):
        # Check that IDs and descriptions start with the prefix.
        assert record["id"].startswith(prefix)
        assert record["desc"].startswith(prefix)
        # Check sequence length constraints.
        seq_len = len(record["seq"])
        assert min_length <= seq_len <= max_length
        for ch in record["seq"]:
            assert ch in "ACGT"