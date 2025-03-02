import os
import pytest
from flowline import (
    GenerateRandomMotifsPipe,
    ParsePWMPipe,
    SampleMotifsFromPWMPipe,
    ProcessProvidedMotifPipe
)

# ------------------------------
# Helper Functions for Testing PWM Files
# ------------------------------

def create_test_pwm_file(tmp_path, content: str) -> str:
    """
    Creates a temporary PWM file with the provided content and returns its file path.
    """
    pwm_file = tmp_path / "test_pwm.txt"
    pwm_file.write_text(content)
    return str(pwm_file)

@pytest.fixture
def test_pwm_content():
    """
    Returns a sample PWM file content with a motif width of 4.
    """
    content = (
        "#INCLUSive Motif Model\n"
        "#ID = test_motif\n"
        "#Score 90.8364\n"
        "#W = 4\n"
        "#Consensus = ATGC\n"
        "0.39 0.02 0.02 0.57\n"
        "0.76 0.2  0.02 0.02\n"
        "0.21 0.02 0.02 0.76\n"
        "0.02 0.02 0.2  0.76\n"
        "\n"
    )
    return content

# ------------------------------
# Tests for GenerateRandomMotifsPipe
# ------------------------------

def test_generate_random_motifs():
    """Test generating multiple random motifs of a given length."""
    pipe = GenerateRandomMotifsPipe()
    data = {"amount": 5, "length": 10}
    result = pipe.execute(data)

    assert "motif_strings" in result
    assert len(result["motif_strings"]) == 5  # Ensures the correct amount of motifs are generated
    assert all(len(motif) == 10 for motif in result["motif_strings"])  # Ensures motif length is correct
    assert all(all(nucleotide in "ACGT" for nucleotide in motif) for motif in result["motif_strings"])

def test_generate_random_motifs_invalid_amount():
    """Test generating motifs with an invalid amount (should raise error)."""
    pipe = GenerateRandomMotifsPipe()
    data = {"amount": -5, "length": 10}

    with pytest.raises(ValueError, match="The amount of motifs must be a positive integer."):
        pipe.execute(data)

def test_generate_random_motifs_invalid_length():
    """Test generating motifs with an invalid length (should raise error)."""
    pipe = GenerateRandomMotifsPipe()
    data = {"amount": 5, "length": -10}

    with pytest.raises(ValueError, match="Motif length must be a positive integer."):
        pipe.execute(data)

def test_generate_random_motifs_zero_amount():
    """Test generating zero motifs (should raise error)."""
    pipe = GenerateRandomMotifsPipe()
    data = {"amount": 0, "length": 10}

    with pytest.raises(ValueError, match="The amount of motifs must be a positive integer."):
        pipe.execute(data)

def test_generate_random_motifs_zero_length():
    """Test generating motifs with zero length (should raise error)."""
    pipe = GenerateRandomMotifsPipe()
    data = {"amount": 5, "length": 0}

    with pytest.raises(ValueError, match="Motif length must be a positive integer."):
        pipe.execute(data)

def test_generate_random_motifs_with_defaults():
    """Test generating motifs using default values."""
    pipe = GenerateRandomMotifsPipe(amount=3, length=8)
    # Not providing amount and length in the data
    data = {}
    result = pipe.execute(data)
    assert "motif_strings" in result
    assert len(result["motif_strings"]) == 3  # Should use default amount=3
    assert all(len(motif) == 8 for motif in result["motif_strings"])  # Should use default length=8

# ------------------------------
# Tests for ParsePWMPipe
# ------------------------------

def test_parse_pwm_success(tmp_path, test_pwm_content):
    """Test parsing a valid PWM file."""
    pwm_file_path = create_test_pwm_file(tmp_path, test_pwm_content)
    pipe = ParsePWMPipe()
    data = {"pwm_file_path": pwm_file_path}
    result = pipe.execute(data)

    assert "pwm_matrix" in result
    pwm_matrix = result["pwm_matrix"]
    assert set(pwm_matrix.keys()) == {"A", "C", "G", "T"}
    assert all(len(pwm_matrix[nuc]) == 4 for nuc in "ACGT")

def test_parse_pwm_missing_file():
    """Test parsing a missing PWM file (should raise FileNotFoundError)."""
    pipe = ParsePWMPipe()
    data = {"pwm_file_path": "non_existent_pwm.txt"}

    with pytest.raises(FileNotFoundError, match="PWM file not found"):
        pipe.execute(data)

def test_parse_pwm_invalid_format(tmp_path):
    """Test parsing a PWM file with incorrect formatting (should raise ValueError)."""
    invalid_pwm_content = "#This is an invalid PWM\n0.2 0.3 0.5\n"
    pwm_file_path = create_test_pwm_file(tmp_path, invalid_pwm_content)

    pipe = ParsePWMPipe()
    data = {"pwm_file_path": pwm_file_path}

    with pytest.raises(ValueError, match="PWM file does not start with the required header"):
        pipe.execute(data)

# ------------------------------
# Tests for SampleMotifFromPWMPipe
# ------------------------------

def test_sample_multiple_motifs_from_pwm(tmp_path, test_pwm_content):
    """Test sampling multiple motifs from a parsed PWM."""
    pwm_file_path = create_test_pwm_file(tmp_path, test_pwm_content)

    # Parse PWM first
    parse_pipe = ParsePWMPipe()
    pwm_data = parse_pipe.execute({"pwm_file_path": pwm_file_path})

    # Sample multiple motifs from parsed PWM
    sample_pipe = SampleMotifsFromPWMPipe()
    result = sample_pipe.execute({"pwm_matrix": pwm_data["pwm_matrix"], "amount": 5})

    assert "motif_strings" in result
    assert len(result["motif_strings"]) == 5
    for motif in result["motif_strings"]:
        assert len(motif) == 4  # Motif length should match PWM width
        assert all(nucleotide in "ACGT" for nucleotide in motif)

def test_sample_motif_from_pwm_with_defaults():
    """Test sampling motifs with a default amount."""
    # Create a sample PWM matrix directly
    pwm_matrix = {
        "A": [0.25, 0.25, 0.25, 0.25],
        "C": [0.25, 0.25, 0.25, 0.25],
        "G": [0.25, 0.25, 0.25, 0.25],
        "T": [0.25, 0.25, 0.25, 0.25]
    }
    pipe = SampleMotifsFromPWMPipe(amount=4)
    result = pipe.execute({"pwm_matrix": pwm_matrix})
    assert "motif_strings" in result
    assert len(result["motif_strings"]) == 4  # Should use default amount=4
    for motif in result["motif_strings"]:
        assert len(motif) == 4
        assert all(nucleotide in "ACGT" for nucleotide in motif)

def test_sample_motif_from_pwm_invalid_amount(tmp_path, test_pwm_content):
    """Test sampling motifs with invalid 'amount' (should raise error)."""
    pwm_file_path = create_test_pwm_file(tmp_path, test_pwm_content)

    # Parse PWM first
    parse_pipe = ParsePWMPipe()
    pwm_data = parse_pipe.execute({"pwm_file_path": pwm_file_path})

    sample_pipe = SampleMotifsFromPWMPipe()

    with pytest.raises(ValueError, match="Amount of samples must be a positive integer."):
        sample_pipe.execute({"pwm_matrix": pwm_data["pwm_matrix"], "amount": -1})

    with pytest.raises(ValueError, match="Amount of samples must be a positive integer."):
        sample_pipe.execute({"pwm_matrix": pwm_data["pwm_matrix"], "amount": 0})

    with pytest.raises(ValueError, match="Amount of samples must be a positive integer."):
        sample_pipe.execute({"pwm_matrix": pwm_data["pwm_matrix"], "amount": "five"})

def test_sample_motif_from_invalid_pwm():
    """Test sampling motifs from an invalid PWM dictionary (should raise error)."""
    pipe = SampleMotifsFromPWMPipe()
    invalid_pwm_data = {"pwm_matrix": {"A": [0.25, 0.25], "C": [0.25], "G": [0.25], "T": [0.25]}, "amount": 3}

    with pytest.raises(ValueError, match="Inconsistent PWM row lengths."):
        pipe.execute(invalid_pwm_data)

# ------------------------------
# Tests for ProcessProvidedMotifPipe
# ------------------------------

def test_process_provided_motif():
    """Test processing a correct motif string."""
    pipe = ProcessProvidedMotifPipe()
    data = {"motif_string": "ACGTACGT"}
    result = pipe.execute(data)

    assert "motif_strings" in result
    assert isinstance(result["motif_strings"], list)
    assert len(result["motif_strings"]) == 1
    assert result["motif_strings"][0] == "ACGTACGT"

def test_process_lowercase_motif():
    """Test processing a lowercase motif string (should convert to uppercase)."""
    pipe = ProcessProvidedMotifPipe()
    data = {"motif_string": "acgtacgt"}
    result = pipe.execute(data)

    assert "motif_strings" in result
    assert result["motif_strings"][0] == "ACGTACGT"

def test_process_invalid_motif():
    """Test processing an incorrect motif string (should raise error)."""
    pipe = ProcessProvidedMotifPipe()
    data = {"motif_string": "XYZ"}

    with pytest.raises(ValueError, match="Invalid motif character 'X' in provided motif."):
        pipe.execute(data)