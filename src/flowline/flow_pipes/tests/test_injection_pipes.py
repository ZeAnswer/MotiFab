import pytest
from flowline import InjectMotifsIntoFastaRecordsPipe

# Sample FASTA records for testing
FASTA_RECORDS = [
    {"id": "seq1", "desc": "First sequence", "seq": "ACGTACGTACGT"},
    {"id": "seq2", "desc": "Second sequence", "seq": "TTGCAAGGTTGCAA"},
    {"id": "seq3", "desc": "Third sequence", "seq": "GATCGATCGATC"},
]

# Sample motifs
MOTIFS = ["AAA", "CCC", "GGG"]

# ------------------------------
# Tests for InjectMotifsIntoFastaRecordsPipe
# ------------------------------

def test_inject_motifs_fixed_count():
    """Test injecting motifs into a fixed number of records."""
    pipe = InjectMotifsIntoFastaRecordsPipe()
    result = pipe.execute({"fasta_records": FASTA_RECORDS, "motif_strings": MOTIFS, "amount": 2})
    
    assert "fasta_records" in result
    injected_records = result["fasta_records"]
    assert len(injected_records) == len(FASTA_RECORDS)  # All records should be present
    
    # Convert original records to a dictionary for easy lookup
    original_records_dict = {rec["id"]: rec["seq"] for rec in FASTA_RECORDS}
    
    # Count how many records were actually injected with a motif
    injected_count = sum(
        1 for rec in injected_records if rec["seq"] != original_records_dict[rec["id"]]
    )
    assert injected_count == 2  # Exactly 2 records should be injected

def test_inject_motifs_all_records():
    """Test injecting motifs into all records."""
    pipe = InjectMotifsIntoFastaRecordsPipe()
    result = pipe.execute({"fasta_records": FASTA_RECORDS, "motif_strings": MOTIFS, "amount": 3})
    
    assert "fasta_records" in result
    injected_records = result["fasta_records"]
    assert len(injected_records) == len(FASTA_RECORDS)
    
    # Convert original records to a dictionary for easy lookup
    original_records_dict = {rec["id"]: rec["seq"] for rec in FASTA_RECORDS}
    
    # Count how many records were actually injected with a motif
    injected_count = sum(
        1 for rec in injected_records if rec["seq"] != original_records_dict[rec["id"]]
    )
    assert injected_count == len(FASTA_RECORDS)  # All records should be injected

def test_inject_motifs_no_injection():
    """Test with 0 injection amount, expecting no sequence modification."""
    pipe = InjectMotifsIntoFastaRecordsPipe()
    result = pipe.execute({"fasta_records": FASTA_RECORDS, "motif_strings": MOTIFS, "amount": 0})
    
    assert "fasta_records" in result
    
    # Check that none of the sequences were modified
    original_records_dict = {rec["id"]: rec["seq"] for rec in FASTA_RECORDS}
    for rec in result["fasta_records"]:
        assert rec["seq"] == original_records_dict[rec["id"]]

def test_inject_motifs_more_than_available():
    """Test injecting more records than available (should inject all)."""
    pipe = InjectMotifsIntoFastaRecordsPipe()
    result = pipe.execute({"fasta_records": FASTA_RECORDS, "motif_strings": MOTIFS, "amount": 10})
    
    assert "fasta_records" in result
    injected_records = result["fasta_records"]
    assert len(injected_records) == len(FASTA_RECORDS)
    
    # Convert original records to a dictionary for easy lookup
    original_records_dict = {rec["id"]: rec["seq"] for rec in FASTA_RECORDS}
    
    # Count how many records were actually injected with a motif
    injected_count = sum(
        1 for rec in injected_records if rec["seq"] != original_records_dict[rec["id"]]
    )
    assert injected_count == len(FASTA_RECORDS)  # All records should be injected

def test_inject_motifs_empty_motif_list():
    """Test injecting with an empty motif list (should raise an error)."""
    pipe = InjectMotifsIntoFastaRecordsPipe()
    with pytest.raises(ValueError, match="motif_strings cannot be empty."):
        pipe.execute({"fasta_records": FASTA_RECORDS, "motif_strings": [], "amount": 2})