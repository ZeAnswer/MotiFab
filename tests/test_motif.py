import os
import pytest
from src.motif import Motif, generate_random_motif, parse_pwm_file, validate_motif_string

# ---------------------------------------------------------------------------
# Helper Functions for Testing PWM Files
# ---------------------------------------------------------------------------

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
    The file includes the required header and comment lines, followed by 4 rows of PWM values,
    and ends with a blank line.
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

# ---------------------------------------------------------------------------
# Tests for Provided Motif Mode
# ---------------------------------------------------------------------------

def test_provided_valid():
    """
    Test that a valid provided motif string is accepted.
    """
    motif_str = "ACGTACGT"
    m = Motif(motif_str, input_type="string")
    result = m.get_motif()
    assert result == motif_str

def test_provided_invalid():
    """
    Test that an invalid motif string (with non-nucleotide characters) is rejected.
    """
    with pytest.raises(ValueError):
        # "helloWorld" contains characters other than A, C, G, T.
        m = Motif("helloWorld", input_type="auto")
        m.get_motif()

# ---------------------------------------------------------------------------
# Tests for Random Motif Mode
# ---------------------------------------------------------------------------

def test_random_motif():
    """
    Test that a random motif is generated correctly.
    The motif should have the specified length, contain only A, C, G, T,
    and remain constant on repeated calls (since it is generated once upon initialization).
    """
    length = 10
    m = Motif(length, input_type="length")
    result = m.get_motif()
    assert len(result) == length
    for ch in result:
        assert ch in "ACGT"
    # Repeated calls return the same motif.
    assert m.get_motif() == result

# ---------------------------------------------------------------------------
# Tests for PWM Mode
# ---------------------------------------------------------------------------

def test_pwm_mode_with_file(tmp_path, test_pwm_content):
    """
    Test that PWM mode correctly parses a PWM file and samples a motif.
    The sampled motif should have length 4 (as specified by the PWM file).
    """
    pwm_file_path = create_test_pwm_file(tmp_path, test_pwm_content)
    m = Motif(pwm_file_path, input_type="file")
    result = m.get_motif()
    assert len(result) == 4
    for ch in result:
        assert ch in "ACGT"
    # In PWM mode, repeated calls may return different motifs.
    result2 = m.get_motif()
    assert len(result2) == 4
    for ch in result2:
        assert ch in "ACGT"

# ---------------------------------------------------------------------------
# Tests for Auto-Detection Mode
# ---------------------------------------------------------------------------

def test_auto_mode_file(tmp_path, test_pwm_content):
    """
    Test that in auto mode, if the input string corresponds to an existing file,
    the mode is interpreted as PWM.
    """
    pwm_file_path = create_test_pwm_file(tmp_path, test_pwm_content)
    m = Motif(pwm_file_path, input_type="auto")
    assert m.mode == "pwm"
    result = m.get_motif()
    assert len(result) == 4
    for ch in result:
        assert ch in "ACGT"
        
def test_pwm_empirical_distribution(tmp_path, test_pwm_content):
    """
    Sample 1,000 motifs from a PWM file and build an empirical frequency matrix.
    Compare the empirical frequencies (for each nucleotide at each position) to the theoretical
    frequencies from the PWM file. Assert that the differences are within a specified tolerance.
    """
    num_samples = 1000
    tolerance = 0.05  # Adjust tolerance as appropriate.
    
    # Create a temporary PWM file.
    pwm_file_path = create_test_pwm_file(tmp_path, test_pwm_content)
    
    # Parse the PWM file to get the theoretical frequencies.
    theoretical_pwm = parse_pwm_file(pwm_file_path)
    motif_width = len(theoretical_pwm["A"])
    
    # Initialize count dictionary for each nucleotide at each position.
    empirical_counts = {nt: [0] * motif_width for nt in "ACGT"}
    
    # Create the Motif object in PWM mode.
    m = Motif(pwm_file_path, input_type="file")
    
    # Sample num_samples motifs and count nucleotide occurrences at each position.
    for _ in range(num_samples):
        sample = m.get_motif()
        assert len(sample) == motif_width
        for pos, ch in enumerate(sample):
            empirical_counts[ch][pos] += 1
    
    # Convert counts to frequencies.
    empirical_freq = {}
    for nt in "ACGT":
        empirical_freq[nt] = [count / num_samples for count in empirical_counts[nt]]
    
    # Check that the empirical frequencies are close to the theoretical frequencies.
    for nt in "ACGT":
        for pos in range(motif_width):
            theor = theoretical_pwm[nt][pos]
            emp = empirical_freq[nt][pos]
            diff = abs(emp - theor)
            assert diff < tolerance, (
                f"At position {pos} for nucleotide {nt}: "
                f"theoretical = {theor:.3f}, empirical = {emp:.3f}, diff = {diff:.3f} exceeds tolerance {tolerance}"
            )


def test_auto_mode_string():
    """
    Test that in auto mode, if the input string does not correspond to an existing file,
    it is interpreted as a provided motif.
    """
    motif_str = "ACGTACGT"
    m = Motif(motif_str, input_type="auto")
    assert m.mode == "provided"
    result = m.get_motif()
    assert result == motif_str

# ---------------------------------------------------------------------------
# Tests for Error Conditions
# ---------------------------------------------------------------------------

def test_file_mode_nonexistent():
    """
    Test that specifying input_type "file" with a non-existent file path raises an error.
    """
    with pytest.raises(ValueError):
        m = Motif("nonexistent_file.txt", input_type="file")
        m.get_motif()

def test_invalid_input_type():
    """
    Test that if the motif input is not of type int or str, a ValueError is raised.
    """
    with pytest.raises(ValueError):
        m = Motif([1, 2, 3], input_type="auto")
        m.get_motif()

def test_explicit_string_invalid():
    """
    Test that if input_type is explicitly "string" and the motif contains invalid characters,
    a ValueError is raised.
    """
    with pytest.raises(ValueError):
        m = Motif("XYZ", input_type="string")
        m.get_motif()