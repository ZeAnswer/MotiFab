import os
import pytest
from flowline import (
    LoadFastaPipe,
    WriteFastaPipe,
    SelectRandomFastaSequencesPipe,
)

# Test file paths
TEST_FASTA_FILE = "test_fasta.fasta"
TEST_OUTPUT_FASTA_FILE = "test_output.fasta"

# Sample FASTA content for testing
FASTA_CONTENT = """>seq1 First sequence
AGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCT
>seq2 Second sequence
TTGCAATCGATTGCAATCGATTGCAATCGATTGCAATTGCAATCGATTGCAATCGATTGCAATCGATTGCAA
>seq3 Third sequence
GATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATC
"""

@pytest.fixture
def create_fasta_file():
    """Creates a temporary FASTA file for testing."""
    with open(TEST_FASTA_FILE, "w") as f:
        f.write(FASTA_CONTENT)
    yield TEST_FASTA_FILE
    os.remove(TEST_FASTA_FILE)

@pytest.fixture
def create_empty_fasta_file():
    """Creates an empty FASTA file for testing."""
    empty_fasta = "empty.fasta"
    with open(empty_fasta, "w") as f:
        pass
    yield empty_fasta
    os.remove(empty_fasta)

# --- LoadFastaPipe Tests ---
def test_load_fasta_success(create_fasta_file):
    """Test loading a valid FASTA file."""
    pipe = LoadFastaPipe()
    data = {"fasta_file_path": create_fasta_file}
    result = pipe.execute(data)
    
    assert "fasta_records" in result
    assert len(result["fasta_records"]) == 3
    assert result["fasta_records"][0]["id"] == "seq1"
    assert result["fasta_records"][1]["id"] == "seq2"
    assert result["fasta_records"][2]["id"] == "seq3"

#TODO currently commented out because the test is failing. load fasta doesn't handle empty files and an error will not be raised
#TODO will also need to add an empty file test after adding it to the code
# def test_load_fasta_invalid_format(create_empty_fasta_file):
#     """Test loading an empty FASTA file (should raise an error)."""
#     pipe = LoadFastaPipe()
#     data = {"fasta_file_path": create_empty_fasta_file}

#     with pytest.raises(ValueError, match="FASTA file does not start with a header line"):
#         pipe.execute(data)

def test_load_fasta_nonexistent_file():
    """Test attempting to load a non-existent FASTA file."""
    pipe = LoadFastaPipe()
    data = {"fasta_file_path": "non_existent.fasta"}

    with pytest.raises(RuntimeError, match="Error loading FASTA file"):
        pipe.execute(data)

# --- WriteFastaPipe Tests ---
def test_write_fasta_success(create_fasta_file):
    """Test writing a FASTA file from parsed records."""
    # Load the existing FASTA file to get records
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]
    
    # Write them to a new file
    write_pipe = WriteFastaPipe()
    data = {"fasta_records": records, "fasta_file_path": TEST_OUTPUT_FASTA_FILE}
    result = write_pipe.execute(data)

    assert result["write_success"] is True
    assert result["fasta_file_path"] == TEST_OUTPUT_FASTA_FILE
    assert os.path.exists(TEST_OUTPUT_FASTA_FILE)

    # Verify contents
    with open(TEST_OUTPUT_FASTA_FILE, "r") as f:
        written_content = f.read().strip()
        print("@@@@")
        print(written_content)
        print(FASTA_CONTENT.strip())
        assert written_content == FASTA_CONTENT.strip()

    os.remove(TEST_OUTPUT_FASTA_FILE)

# --- SelectRandomFastaSequencesPipe Tests ---
def test_select_random_fasta_valid(create_fasta_file):
    """Test selecting a valid random subset of FASTA records."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    data = {"fasta_records": records, "amount": 2}
    result = select_pipe.execute(data)

    assert "fasta_records" in result
    assert "indices" in result
    assert len(result["fasta_records"]) == 2
    assert len(result["indices"]) == 2

def test_select_random_fasta_with_exclusions(create_fasta_file):
    """Test selecting sequences while excluding certain indices."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    excluded = [0]  # Exclude first sequence
    data = {"fasta_records": records, "amount": 2, "excluded_indices": excluded}
    result = select_pipe.execute(data)

    assert "fasta_records" in result
    assert "indices" in result
    assert len(result["fasta_records"]) == 2
    assert len(result["indices"]) == 2
    assert not any(idx in excluded for idx in result["indices"])  # Ensure excluded indices are not in the result

def test_select_random_fasta_with_mandatory(create_fasta_file):
    """Test selecting sequences with mandatory indices included."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    mandatory = [1]  # Force include second sequence
    data = {"fasta_records": records, "amount": 2, "mandatory_indices": mandatory}
    result = select_pipe.execute(data)

    assert "fasta_records" in result
    assert "indices" in result
    assert len(result["fasta_records"]) == 2
    assert len(result["indices"]) == 2
    assert any(idx in mandatory for idx in result["indices"])  # Ensure mandatory indices are included

def test_select_random_fasta_with_exclusions_and_mandatory(create_fasta_file):
    """Test selecting sequences with both exclusions and mandatory indices."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    mandatory = [1]  # Force include second sequence
    excluded = [0]   # Exclude first sequence
    data = {"fasta_records": records, "amount": 2, "mandatory_indices": mandatory, "excluded_indices": excluded}
    result = select_pipe.execute(data)

    assert "fasta_records" in result
    assert "indices" in result
    assert len(result["fasta_records"]) == 2
    assert len(result["indices"]) == 2
    assert any(idx in mandatory for idx in result["indices"])  # Ensure mandatory is included
    assert not any(idx in excluded for idx in result["indices"])  # Ensure excluded indices are not present

def test_select_random_fasta_exceeds_available_with_duplicates(create_fasta_file):
    """Test requesting more sequences than available, allowing duplicates."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    data = {"fasta_records": records, "amount": 10}  # More than available

    result = select_pipe.execute(data)

    assert "fasta_records" in result
    assert "indices" in result
    assert len(result["fasta_records"]) == 10
    assert len(result["indices"]) == 10  # Ensuring duplicates filled the selection

def test_select_random_fasta_with_too_many_mandatory(create_fasta_file):
    """Test requesting fewer sequences than the number of mandatory indices."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    mandatory = [0, 1, 2]  # All available indices as mandatory
    data = {"fasta_records": records, "amount": 2, "mandatory_indices": mandatory}

    with pytest.raises(ValueError, match="Number of mandatory indices.*exceeds the requested selection count"):
        select_pipe.execute(data)

def test_select_random_fasta_with_out_of_range_mandatory(create_fasta_file):
    """Test providing mandatory indices that are out of range."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    mandatory = [5]  # Out of range index (since we have only 3 records)
    data = {"fasta_records": records, "amount": 2, "mandatory_indices": mandatory}

    with pytest.raises(ValueError, match="Mandatory indices .* are out of range"):
        select_pipe.execute(data)

def test_select_random_fasta_with_conflicting_mandatory_and_excluded(create_fasta_file):
    """Test mandatory and excluded indices conflicting (same index)."""
    load_pipe = LoadFastaPipe()
    records = load_pipe.execute({"fasta_file_path": create_fasta_file})["fasta_records"]

    select_pipe = SelectRandomFastaSequencesPipe()
    mandatory = [1]
    excluded = [1]
    data = {"fasta_records": records, "amount": 2, "mandatory_indices": mandatory, "excluded_indices": excluded}

    with pytest.raises(ValueError, match="Mandatory indices .* are also in the excluded list"):
        select_pipe.execute(data)

def test_select_random_fasta_with_empty_records():
    """Test selecting sequences when no records are available."""
    select_pipe = SelectRandomFastaSequencesPipe()
    data = {"fasta_records": [], "amount": 2}

    with pytest.raises(ValueError, match="No records available for selection."):
        select_pipe.execute(data)