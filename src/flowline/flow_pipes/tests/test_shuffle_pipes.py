import pytest
from flowline import NaiveShufflePipe, DiPairShufflePipe

# Sample FASTA records for testing
SAMPLE_FASTA_RECORDS = [
    {"id": "seq1", "desc": "First sequence", "seq": "ACGTACGT"},
    {"id": "seq2", "desc": "Second sequence", "seq": "TTGCAAGG"},
    {"id": "seq3", "desc": "Third sequence", "seq": "GATCGATC"}
]

# ------------------------------
# Tests for NaiveShufflePipe
# ------------------------------

def test_naive_shuffle():
    """Test naive shuffling maintains sequence length and composition for FASTA records."""
    pipe = NaiveShufflePipe()
    result = pipe.execute({"fasta_records": SAMPLE_FASTA_RECORDS})
    
    assert "fasta_records" in result
    shuffled_records = result["fasta_records"]
    
    assert len(shuffled_records) == len(SAMPLE_FASTA_RECORDS)  # Ensure the same number of records
    
    for original, shuffled in zip(SAMPLE_FASTA_RECORDS, shuffled_records):
        assert shuffled["id"] == original["id"]  # Ensure ID is preserved
        assert shuffled["desc"] == original["desc"]  # Ensure description is preserved
        assert len(shuffled["seq"]) == len(original["seq"])  # Ensure length is preserved
        assert sorted(shuffled["seq"]) == sorted(original["seq"])  # Ensure same nucleotide composition

def test_naive_shuffle_empty_list():
    """Test naive shuffle with an empty list (should raise error)."""
    pipe = NaiveShufflePipe()
    with pytest.raises(ValueError, match="Input FASTA records list cannot be empty."):
        pipe.execute({"fasta_records": []})

def test_naive_shuffle_empty_sequence_in_record():
    """Test naive shuffle with an empty sequence in a record (should raise error)."""
    empty_seq_record = [{"id": "empty", "desc": "Empty sequence", "seq": ""}]
    pipe = NaiveShufflePipe()
    with pytest.raises(ValueError, match="A sequence in the FASTA records is empty."):
        pipe.execute({"fasta_records": empty_seq_record})

# ------------------------------
# Tests for DiPairShufflePipe
# ------------------------------

def test_di_pair_shuffle_even_length():
    """Test di-pair shuffling for FASTA records with even-length sequences."""
    pipe = DiPairShufflePipe()
    result = pipe.execute({"fasta_records": SAMPLE_FASTA_RECORDS})
    
    assert "fasta_records" in result
    shuffled_records = result["fasta_records"]
    
    assert len(shuffled_records) == len(SAMPLE_FASTA_RECORDS)  # Ensure the same number of records
    
    for original, shuffled in zip(SAMPLE_FASTA_RECORDS, shuffled_records):
        assert shuffled["id"] == original["id"]  # Ensure ID is preserved
        assert shuffled["desc"] == original["desc"]  # Ensure description is preserved
        assert len(shuffled["seq"]) == len(original["seq"])  # Ensure length is preserved
        assert sorted(shuffled["seq"]) == sorted(original["seq"])  # Ensure same nucleotide composition

def test_di_pair_shuffle_odd_length():
    """Test di-pair shuffling for FASTA records with odd-length sequences."""
    odd_length_records = [
        {"id": "odd1", "desc": "Odd sequence 1", "seq": "ACGTACGTA"},
        {"id": "odd2", "desc": "Odd sequence 2", "seq": "TGCAA"}
    ]
    
    pipe = DiPairShufflePipe()
    result = pipe.execute({"fasta_records": odd_length_records})
    
    assert "fasta_records" in result
    shuffled_records = result["fasta_records"]
    
    assert len(shuffled_records) == len(odd_length_records)
    
    for original, shuffled in zip(odd_length_records, shuffled_records):
        assert shuffled["id"] == original["id"]
        assert shuffled["desc"] == original["desc"]
        assert len(shuffled["seq"]) == len(original["seq"])
        assert sorted(shuffled["seq"]) == sorted(original["seq"])

def test_di_pair_shuffle_empty_list():
    """Test di-pair shuffle with an empty list (should raise error)."""
    pipe = DiPairShufflePipe()
    with pytest.raises(ValueError, match="Input FASTA records list cannot be empty."):
        pipe.execute({"fasta_records": []})

def test_di_pair_shuffle_empty_sequence_in_record():
    """Test di-pair shuffle with an empty sequence in a record (should raise error)."""
    empty_seq_record = [{"id": "empty", "desc": "Empty sequence", "seq": ""}]
    pipe = DiPairShufflePipe()
    with pytest.raises(ValueError, match="A sequence in the FASTA records is empty."):
        pipe.execute({"fasta_records": empty_seq_record})