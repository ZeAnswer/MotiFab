import random
import pytest
from src.sequence_injector import inject_motif, inject_motif_into_records

def test_inject_motif_too_short():
    """
    If the sequence is shorter than the motif, inject_motif should return the original sequence.
    """
    seq = "ACG"
    motif = "ACGT"
    result = inject_motif(seq, motif)
    assert result == seq

def test_inject_motif_normal_injection():
    """
    Test that inject_motif replaces a substring of the correct length.
    The result should have the same length as the original sequence,
    and the injected motif must appear in the result.
    """
    seq = "ACGTACGTACGT"
    motif = "TTTT"
    result = inject_motif(seq, motif)
    assert len(result) == len(seq)
    assert motif in result

def test_inject_motif_into_records_percentage():
    """
    Test that when using a percentage (e.g., "50%"), approximately the correct number of records get injected.
    """
    # Create a list of dummy FASTA records.
    records = [
        {"id": "1", "desc": "1", "seq": "ACGTACGTACGT"},
        {"id": "2", "desc": "2", "seq": "TTTTGGGGCCC"},
        {"id": "3", "desc": "3", "seq": "AAAACCC"},
        {"id": "4", "desc": "4", "seq": "GGGGTTTT"}
    ]
    motif = "CCCC"
    injection_rate = "50%"  # Expect about 50% of the records injected.
    
    new_records = inject_motif_into_records(records, motif, injection_rate)
    injected_count = sum(1 for rec in new_records if motif in rec["seq"])
    expected_count = int(round(len(records) * 0.5))
    # Allow a difference of at most 1 due to rounding.
    assert abs(injected_count - expected_count) <= 1

def test_inject_motif_into_records_absolute():
    """
    Test that when using an absolute number (e.g., "2"), exactly that many records get injected.
    """
    records = [
        {"id": "1", "desc": "1", "seq": "ACGTACGTACGT"},
        {"id": "2", "desc": "2", "seq": "TTTTGGGCCCC"},
        {"id": "3", "desc": "3", "seq": "AAAACCCC"}
    ]
    motif = "GGGG"
    injection_rate = "2"  # Absolute injection into 2 records.
    
    new_records = inject_motif_into_records(records, motif, injection_rate)
    injected_count = sum(1 for rec in new_records if motif in rec["seq"])
    print(injected_count)
    assert injected_count == 2
