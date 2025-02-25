import pytest
from flowline import NaiveShufflePipe, DiPairShufflePipe

# Sample sequences for testing
SAMPLE_SEQUENCES = ["ACGTACGT", "TTGCAAGG", "GATCGATC"]

# ------------------------------
# Tests for NaiveShufflePipe
# ------------------------------

def test_naive_shuffle():
    """Test naive shuffling maintains sequence length and composition for a list of sequences."""
    pipe = NaiveShufflePipe()
    result = pipe.execute({"sequences": SAMPLE_SEQUENCES})

    assert "sequences" in result
    shuffled_sequences = result["sequences"]
    
    assert len(shuffled_sequences) == len(SAMPLE_SEQUENCES)  # Ensure the same number of sequences
    for original, shuffled in zip(SAMPLE_SEQUENCES, shuffled_sequences):
        assert len(shuffled) == len(original)
        assert sorted(shuffled) == sorted(original)  # Ensure same nucleotide composition

def test_naive_shuffle_empty_list():
    """Test naive shuffle with an empty list (should raise error)."""
    pipe = NaiveShufflePipe()
    with pytest.raises(ValueError, match="Input sequences list cannot be empty."):
        pipe.execute({"sequences": []})

def test_naive_shuffle_empty_sequence_in_list():
    """Test naive shuffle with an empty sequence in the list (should raise error)."""
    pipe = NaiveShufflePipe()
    with pytest.raises(ValueError, match="A sequence in the list is empty."):
        pipe.execute({"sequences": ["ACGT", ""]})

# ------------------------------
# Tests for DiPairShufflePipe
# ------------------------------

def test_di_pair_shuffle_even_length():
    """Test di-pair shuffling for a list of even-length sequences."""
    pipe = DiPairShufflePipe()
    result = pipe.execute({"sequences": SAMPLE_SEQUENCES})

    assert "sequences" in result
    shuffled_sequences = result["sequences"]

    assert len(shuffled_sequences) == len(SAMPLE_SEQUENCES)  # Ensure the same number of sequences
    for original, shuffled in zip(SAMPLE_SEQUENCES, shuffled_sequences):
        assert len(shuffled) == len(original)
        assert sorted(shuffled) == sorted(original)  # Ensure same nucleotide composition

def test_di_pair_shuffle_odd_length():
    """Test di-pair shuffling for a list that contains odd-length sequences."""
    odd_sequences = ["ACGTACGTA", "TGCAA"]
    pipe = DiPairShufflePipe()
    result = pipe.execute({"sequences": odd_sequences})

    assert "sequences" in result
    shuffled_sequences = result["sequences"]

    assert len(shuffled_sequences) == len(odd_sequences)
    for original, shuffled in zip(odd_sequences, shuffled_sequences):
        assert len(shuffled) == len(original)
        assert sorted(shuffled) == sorted(original)

def test_di_pair_shuffle_empty_list():
    """Test di-pair shuffle with an empty list (should raise error)."""
    pipe = DiPairShufflePipe()
    with pytest.raises(ValueError, match="Input sequences list cannot be empty."):
        pipe.execute({"sequences": []})

def test_di_pair_shuffle_empty_sequence_in_list():
    """Test di-pair shuffle with an empty sequence in the list (should raise error)."""
    pipe = DiPairShufflePipe()
    with pytest.raises(ValueError, match="A sequence in the list is empty."):
        pipe.execute({"sequences": ["ACGT", ""]})