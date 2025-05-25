import os
import re
import pytest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from flowline import MemeCommandGeneratorPipe, SlurmJobGeneratorPipe, FlowSplitJoinPipe#, FlowParallelPipe
from flowline.flow_pipes.enrichment_pipes import BatchJobExecutorPipe, JobExecutorPipe, HomerCommandGeneratorPipe

# Test data
TEST_FASTA_CONTENT = """>seq1 Test sequence 1
ACGTACGTACGTACGTACGTACGT
>seq2 Test sequence 2
TTGCAATCGATTGCAATCGATCG
>seq3 Test sequence 3
GATCGATCGATCGATCGATCGATC
"""

@pytest.fixture
def test_fasta_files():
    """Create temporary test and background FASTA files."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fasta') as test_file:
        test_file.write(TEST_FASTA_CONTENT)
        test_path = test_file.name
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.fasta') as bg_file:
        bg_file.write(TEST_FASTA_CONTENT)
        bg_path = bg_file.name
    
    yield test_path, bg_path
    
    # Clean up
    os.unlink(test_path)
    os.unlink(bg_path)

@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for output files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

# --- MemeCommandGeneratorPipe Tests ---

def test_meme_command_generator_basic(test_fasta_files, temp_output_dir):
    """Test basic MEME command generation with default parameters."""
    test_path, bg_path = test_fasta_files
    
    pipe = MemeCommandGeneratorPipe(output_dir_prefix=temp_output_dir)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "test_run1"}
    }
    
    result = pipe.execute(data)
    
    # Check outputs
    assert "command" in result
    assert "output_dir" in result
    assert "run_id" in result
    
    # Check output values
    assert test_path in result["command"]
    assert bg_path in result["command"]
    assert os.path.join(temp_output_dir, "test_run1") == result["output_dir"]
    assert os.path.join(temp_output_dir, "test_run1") in result["command"]
    assert "-dna" in result["command"]
    assert "-revcomp" in result["command"]
    assert "-oc" in result["command"]
    assert result["run_id"] == "test_run1"
    
    # Check that output directory was created
    assert os.path.exists(result["output_dir"])

def test_meme_command_generator_custom_params(test_fasta_files, temp_output_dir):
    """Test MEME command generation with custom parameters."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with custom MEME parameters
    custom_params = {
        "dna": False,  # Override default
        "revcomp": False,  # Override default
        "nmotifs": 5,
        "minw": 8,
        "maxw": 15,
        "mod": "zoops"
    }
    
    pipe = MemeCommandGeneratorPipe(meme_params=custom_params, output_dir_prefix=temp_output_dir)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "custom_params_run"}
    }
    
    result = pipe.execute(data)
    
    # Check command reflects custom parameters
    assert "-dna" not in result["command"]
    assert "-revcomp" not in result["command"]
    assert "-nmotifs 5" in result["command"]
    assert "-minw 8" in result["command"]
    assert "-maxw 15" in result["command"]
    assert "-mod zoops" in result["command"]
    assert result["run_id"] == "custom_params_run"

def test_meme_command_generator_extra_params(test_fasta_files, temp_output_dir):
    """Test MEME command generation with extra parameters."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with extra parameters
    extra_params = "-seed 12345 -maxiter 1000"
    
    pipe = MemeCommandGeneratorPipe(output_dir_prefix=temp_output_dir, extra_params=extra_params)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "extra_params_run"}
    }
    
    result = pipe.execute(data)
    
    # Check that extra parameters are included in the command
    assert "-seed 12345" in result["command"]
    assert "-maxiter 1000" in result["command"]

def test_meme_command_generator_missing_inputs():
    """Test that errors are raised when required inputs are missing."""
    pipe = MemeCommandGeneratorPipe(output_dir_prefix="output")
    
    # Missing test_fasta_path
    with pytest.raises(ValueError, match="Missing required input: test_fasta_path"):
        pipe.execute({
        "summary_record" : {
            "background_fasta_path": "background.fa",
            "run_id": "test1"}
        })
    
    # Missing background_fasta_path
    with pytest.raises(ValueError, match="Missing required input: background_fasta_path"):
        pipe.execute({
        "summary_record" : {
            "test_fasta_path": "test.fa",
            "run_id": "test1"}
        })
    
    # Missing run_id
    with pytest.raises(ValueError, match="Missing required input: run_id"):
        pipe.execute({
        "summary_record" : {
            "test_fasta_path": "test.fa",
            "background_fasta_path": "background.fa"}
        })

def test_meme_command_generator_output_dir_creation(test_fasta_files, temp_output_dir):
    """Test that output directory is created if it doesn't exist."""
    test_path, bg_path = test_fasta_files
    
    # Create a nested output directory path that doesn't exist yet
    nested_dir = os.path.join(temp_output_dir, "nested", "dir")
    run_id = "nested_test"
    expected_output_dir = os.path.join(nested_dir, run_id)
    
    # Verify the directory doesn't exist yet
    assert not os.path.exists(expected_output_dir)
    
    pipe = MemeCommandGeneratorPipe(output_dir_prefix=nested_dir)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": run_id}
    }
    
    result = pipe.execute(data)
    
    # Check output directory was created
    assert os.path.exists(expected_output_dir)
    assert result["output_dir"] == expected_output_dir

def test_meme_command_generator_motif_length_range(test_fasta_files, temp_output_dir):
    """Test MEME command generation with motif length range."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with motif length range
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        motif_length="5-8"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "motif_length_range"}
    }
    
    result = pipe.execute(data)
    
    # Check that motif length parameters are included in the command
    assert "-minw 5" in result["command"]
    assert "-maxw 8" in result["command"]

def test_meme_command_generator_motif_length_list(test_fasta_files, temp_output_dir):
    """Test MEME command generation with motif length list."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with motif length list
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        motif_length="5,6,7,8"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "motif_length_list"}
    }
    
    result = pipe.execute(data)
    
    # Check that motif length parameters are included in the command
    # MEME doesn't support a list directly, so it should use min and max
    assert "-minw 5" in result["command"]
    assert "-maxw 8" in result["command"]

def test_meme_command_generator_motif_length_single(test_fasta_files, temp_output_dir):
    """Test MEME command generation with single motif length."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with single motif length
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        motif_length="5"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "motif_length_single"}
    }
    
    result = pipe.execute(data)
    
    # Check that motif length parameters are included in the command
    assert "-minw 5" in result["command"]
    assert "-maxw 5" in result["command"]

def test_meme_command_generator_num_motifs(test_fasta_files, temp_output_dir):
    """Test MEME command generation with number of motifs."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with number of motifs
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        num_motifs=15
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "num_motifs"}
    }
    
    result = pipe.execute(data)
    
    # Check that number of motifs parameter is included in the command
    assert "-nmotifs 15" in result["command"]

def test_meme_command_generator_strand(test_fasta_files, temp_output_dir):
    """Test MEME command generation with strand specification."""
    test_path, bg_path = test_fasta_files
    
    # Test forward strand
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        strand="+"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "strand_forward"}
    }
    
    result = pipe.execute(data)
    
    # Check that strand parameter is included in the command
    assert "-revcomp" not in result["command"]
    
    # Test reverse strand
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        strand="-"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "strand_reverse"}
    }
    
    result = pipe.execute(data)
    
    # Check that strand parameter is included in the command
    assert "-revcomp" not in result["command"]
    
    # Test both strands
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        strand="both"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "strand_both"}
    }
    
    result = pipe.execute(data)
    
    # Check that strand parameter is included in the command
    assert "-revcomp" in result["command"]

def test_meme_command_generator_revcomp(test_fasta_files, temp_output_dir):
    """Test MEME command generation with revcomp parameter."""
    test_path, bg_path = test_fasta_files
    
    # Test with revcomp=True
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        revcomp=True
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "revcomp_true"}
    }
    
    result = pipe.execute(data)
    
    # Check that revcomp parameter is included in the command
    assert "-revcomp" in result["command"]
    
    # Test with revcomp=False
    pipe = MemeCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        revcomp=False
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "revcomp_false"}
    }
    
    result = pipe.execute(data)
    
    # Check that revcomp parameter is not included in the command
    assert "-revcomp" not in result["command"]

# --- HomerCommandGeneratorPipe Tests ---

def test_homer_command_generator_basic(test_fasta_files, temp_output_dir):
    """Test basic HOMER command generation with default parameters."""
    test_path, bg_path = test_fasta_files
    
    pipe = HomerCommandGeneratorPipe(output_dir_prefix=temp_output_dir)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "test_run1"}
    }
    
    result = pipe.execute(data)
    
    # Check outputs
    assert "command" in result
    assert "output_dir" in result
    assert "run_id" in result
    
    # Check output values
    assert test_path in result["command"]
    assert bg_path in result["command"]
    assert os.path.join(temp_output_dir, "test_run1") == result["output_dir"]
    assert "homer2 denovo" in result["command"]
    assert "-i" in result["command"]
    assert "-b" in result["command"]
    assert "-o" in result["command"]
    assert result["run_id"] == "test_run1"
    
    # Check that output directory was created
    assert os.path.exists(result["output_dir"])

def test_homer_command_generator_custom_params(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with custom parameters."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with custom HOMER parameters
    custom_params = {
        "len": 15,
        "mis": 3,
        "strand": "+",
        "stat": "hypergeo",
        "S": 50,
        "p": 4
    }
    
    pipe = HomerCommandGeneratorPipe(homer_params=custom_params, output_dir_prefix=temp_output_dir)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "custom_params_run"}
    }
    
    result = pipe.execute(data)
    
    # Check command reflects custom parameters
    assert "-len 15" in result["command"]
    assert "-mis 3" in result["command"]
    assert "-strand +" in result["command"]
    assert "-stat hypergeo" in result["command"]
    assert "-S 50" in result["command"]
    assert "-p 4" in result["command"]
    assert result["run_id"] == "custom_params_run"

def test_homer_command_generator_extra_params(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with extra parameters."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with extra parameters
    extra_params = "-fullMask -cache 1000"
    
    pipe = HomerCommandGeneratorPipe(output_dir_prefix=temp_output_dir, extra_params=extra_params)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "extra_params_run"}
    }
    
    result = pipe.execute(data)
    
    # Check that extra parameters are included in the command
    assert "-fullMask" in result["command"]
    assert "-cache 1000" in result["command"]

def test_homer_command_generator_missing_inputs():
    """Test that errors are raised when required inputs are missing."""
    pipe = HomerCommandGeneratorPipe(output_dir_prefix="output")
    
    # Missing test_fasta_path
    with pytest.raises(ValueError, match="Missing required input: test_fasta_path"):
        pipe.execute({
        "summary_record" : {
            "background_fasta_path": "background.fa",
            "run_id": "test1"}
        })
    
    # Missing background_fasta_path
    with pytest.raises(ValueError, match="Missing required input: background_fasta_path"):
        pipe.execute({
        "summary_record" : {
            "test_fasta_path": "test.fa",
            "run_id": "test1"}
        })
    
    # Missing run_id
    with pytest.raises(ValueError, match="Missing required input: run_id"):
        pipe.execute({
        "summary_record" : {
            "test_fasta_path": "test.fa",
            "background_fasta_path": "background.fa"}
        })

def test_homer_command_generator_output_dir_creation(test_fasta_files, temp_output_dir):
    """Test that output directory is created if it doesn't exist."""
    test_path, bg_path = test_fasta_files
    
    # Create a nested output directory path that doesn't exist yet
    nested_dir = os.path.join(temp_output_dir, "nested", "dir")
    run_id = "nested_test"
    expected_output_dir = os.path.join(nested_dir, run_id)
    
    # Verify the directory doesn't exist yet
    assert not os.path.exists(expected_output_dir)
    
    pipe = HomerCommandGeneratorPipe(output_dir_prefix=nested_dir)
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": run_id}
    }
    
    result = pipe.execute(data)
    
    # Check output directory was created
    assert os.path.exists(expected_output_dir)
    assert result["output_dir"] == expected_output_dir

def test_homer_command_generator_motif_length_range(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with motif length range."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with motif length range
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        motif_length="5-8"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "motif_length_range"}
    }
    
    result = pipe.execute(data)
    
    # Check that motif length parameter is included in the command
    # HOMER doesn't support a range directly, so it should use the min length
    assert "-len 5" in result["command"]

def test_homer_command_generator_motif_length_list(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with motif length list."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with motif length list
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        motif_length="5,6,7,8"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "motif_length_list"}
    }
    
    result = pipe.execute(data)
    
    # Check that motif length parameter is included in the command
    # HOMER doesn't support a list directly, so it should use the first length
    assert "-len 5" in result["command"]

def test_homer_command_generator_motif_length_single(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with single motif length."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with single motif length
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        motif_length="5"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "motif_length_single"}
    }
    
    result = pipe.execute(data)
    
    # Check that motif length parameter is included in the command
    assert "-len 5" in result["command"]

def test_homer_command_generator_num_motifs(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with number of motifs."""
    test_path, bg_path = test_fasta_files
    
    # Create a pipe with number of motifs
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        num_motifs=15
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "num_motifs"}
    }
    
    result = pipe.execute(data)
    
    # Check that number of motifs parameter is included in the command
    assert "-S 15" in result["command"]

def test_homer_command_generator_strand(test_fasta_files, temp_output_dir):
    """Test HOMER command generation with strand specification."""
    test_path, bg_path = test_fasta_files
    
    # Test forward strand
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        strand="+"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "strand_forward"}
    }
    
    result = pipe.execute(data)
    
    # Check that strand parameter is included in the command
    assert "-strand +" in result["command"]
    
    # Test reverse strand
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        strand="-"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "strand_reverse"}
    }
    
    result = pipe.execute(data)
    
    # Check that strand parameter is included in the command
    assert "-strand -" in result["command"]
    
    # Test both strands
    pipe = HomerCommandGeneratorPipe(
        output_dir_prefix=temp_output_dir,
        strand="both"
    )
    data = {
        "summary_record" : {
        "test_fasta_path": test_path,
        "background_fasta_path": bg_path,
        "run_id": "strand_both"}
    }
    
    result = pipe.execute(data)
    
    # Check that strand parameter is included in the command
    assert "-strand both" in result["command"]

# --- SlurmJobGeneratorPipe Tests ---

def test_slurm_job_generator_basic(test_fasta_files, temp_output_dir):
    """Test basic SLURM job script generation."""
    test_path, bg_path = test_fasta_files
    
    # First generate MEME command
    meme_pipe = MemeCommandGeneratorPipe(output_dir_prefix=temp_output_dir)
    meme_data = {
        "summary_record": {
            "test_fasta_path": test_path,
            "background_fasta_path": bg_path,
            "run_id": "slurm_test"
        }
    }
    meme_result = meme_pipe.execute(meme_data)
    
    # Now generate SLURM job script
    slurm_pipe = SlurmJobGeneratorPipe(module_name="meme-5.4.1", job_name="test_job")
    data = {
        "command": meme_result["command"],
        "output_dir": meme_result["output_dir"]
    }
    
    result = slurm_pipe.execute(data)
    
    # Check outputs
    assert "job_script" in result
    assert os.path.exists(result["job_script"])
    
    # Check script content
    job_script_path = result["job_script"]
    with open(job_script_path, "r") as f:
        content = f.read()
        assert "#!/bin/bash" in content
        assert "#SBATCH --job-name=test_job" in content
        assert "#SBATCH --time=4:00:00" in content  # Default value
        assert "#SBATCH --mem=16GB" in content  # Default value
        assert "#SBATCH --cpus-per-task=4" in content  # Default value
        assert "module load meme-5.4.1" in content
        assert meme_result["command"] in content

def test_slurm_job_generator_custom_params(test_fasta_files, temp_output_dir):
    """Test SLURM job script generation with custom parameters."""
    test_path, bg_path = test_fasta_files
    
    # Generate MEME command
    meme_pipe = MemeCommandGeneratorPipe(output_dir_prefix=temp_output_dir)
    meme_data = {
        "summary_record": {
            "test_fasta_path": test_path,
            "background_fasta_path": bg_path,
            "run_id": "custom_slurm"
        }
    }
    meme_result = meme_pipe.execute(meme_data)
    
    # Generate SLURM job script with custom parameters
    custom_slurm_params = {
        "time": "12:00:00",
        "mem": "32GB",
        "cpus_per_task": 8,
        "partition": "high-priority"
    }
    
    slurm_pipe = SlurmJobGeneratorPipe(
        job_name="custom_job",
        module_name="meme-5.5.7",
        slurm_params=custom_slurm_params
    )
    
    data = {
        "command": meme_result["command"],
        "output_dir": meme_result["output_dir"]
    }
    
    result = slurm_pipe.execute(data)
    
    # Check script content with custom params
    job_script_path = result["job_script"]
    with open(job_script_path, "r") as f:
        content = f.read()
        assert "#SBATCH --job-name=custom_job" in content
        assert "#SBATCH --time=12:00:00" in content
        assert "#SBATCH --mem=32GB" in content
        assert "#SBATCH --cpus-per-task=8" in content
        assert "#SBATCH --partition=high-priority" in content
        assert "module load meme-5.5.7" in content

def test_slurm_job_generator_missing_input(temp_output_dir):
    """Test that errors are raised when required inputs are missing."""
    slurm_pipe = SlurmJobGeneratorPipe(module_name="meme-5.4.1")
    
    # Missing command
    with pytest.raises(ValueError, match="Missing required input: command"):
        slurm_pipe.execute({
            "output_dir": temp_output_dir
        })
    
    # Missing output_dir
    with pytest.raises(ValueError, match="Missing required input: output_dir"):
        slurm_pipe.execute({
            "command": "meme test.fa"
        })
    
    # Missing module_name (during initialization)
    with pytest.raises(ValueError, match="Module name must be provided during initialization"):
        slurm_pipe = SlurmJobGeneratorPipe()  # No module_name
        slurm_pipe.execute({
            "command": "meme test.fa",
            "output_dir": temp_output_dir
        })

# --- JobExecutorPipe Tests ---

def test_job_executor_pipe_wait_for_completion(temp_output_dir):
    """Test job execution with wait_for_completion=True."""
    # Create a test job script
    job_script_path = os.path.join(temp_output_dir, "test_job.sh")
    with open(job_script_path, "w") as f:
        f.write("#!/bin/bash\necho 'Test job'\n")
    os.chmod(job_script_path, 0o755)
    
    # Mock sequence: first call submits job, second call checks status
    def mock_side_effect(*args, **kwargs):
        if args[0][0] == "sbatch":
            mock_process = MagicMock()
            mock_process.stdout = f"Submitted batch job 12345"
            mock_process.stderr = ""
            return mock_process
        elif args[0][0] == "sacct":
            mock_process = MagicMock()
            mock_process.stdout = f"12345|COMPLETED|0:0\n"
            mock_process.stderr = ""
            return mock_process
    
    with patch("subprocess.run", side_effect=mock_side_effect):
        with patch("time.sleep", return_value=None):  # Skip sleep
            pipe = JobExecutorPipe(wait_for_completion=True, poll_interval=1)
            data = {
                "job_script": job_script_path
            }
            
            result = pipe.execute(data)
            
            # Check outputs
            assert "status" in result
            assert result["status"] == "COMPLETED"

def test_job_executor_pipe_submission_failure(temp_output_dir):
    """Test handling of job submission failures."""
    # Create a test job script
    job_script_path = os.path.join(temp_output_dir, "fail_job.sh")
    with open(job_script_path, "w") as f:
        f.write("#!/bin/bash\necho 'Test job'\n")
    os.chmod(job_script_path, 0o755)
    
    # Mock to simulate a submission failure
    with patch("subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, 
            cmd=["sbatch", job_script_path],
            stderr="Submission failed: invalid partition"
        )
        
        pipe = JobExecutorPipe(wait_for_completion=False)
        data = {
            "job_script": job_script_path
        }
        
        result = pipe.execute(data)
        
        # Check outputs
        assert "status" in result
        assert result["status"] == "ERROR"

def test_job_executor_pipe_missing_input():
    """Test that an error is raised when required inputs are missing."""
    pipe = JobExecutorPipe(wait_for_completion=False)
    
    # Missing job_script
    with pytest.raises(ValueError, match="Missing required input: job_script"):
        pipe.execute({})
    
    # Non-existent job_script
    with pytest.raises(ValueError, match="Job script not found"):
        pipe.execute({
            "job_script": "/path/to/nonexistent/script.sh",
        })

# --- BatchJobExecutorPipe Tests (compatibility tests for the old pipe) ---

def test_batch_job_executor_submit(temp_output_dir):
    """Test batch job submission with mocked subprocess calls."""
    # Create a test job script
    job_script_path = os.path.join(temp_output_dir, "test_job.sh")
    with open(job_script_path, "w") as f:
        f.write("#!/bin/bash\necho 'Test job'\n")
    os.chmod(job_script_path, 0o755)
    
    # Mock the subprocess.run function
    with patch("subprocess.run") as mock_run:
        # Configure the mock for submit_job
        mock_process = MagicMock()
        mock_process.stdout = "Submitted batch job 12345"
        mock_process.stderr = ""
        mock_run.return_value = mock_process
        
        pipe = BatchJobExecutorPipe()
        data = {
            "job_scripts": [job_script_path],
            "max_concurrent": 2,
            "wait_for_completion": False
        }
        
        result = pipe.execute(data)
        
        # Check that the subprocess was called correctly
        mock_run.assert_called_with(
            ["sbatch", job_script_path],
            capture_output=True, text=True, check=True
        )
        
        # Check outputs
        assert "job_ids" in result
        assert result["job_ids"] == ["12345"]
        assert "failed_jobs" in result
        assert not result["failed_jobs"]

def test_batch_job_executor_wait_for_completion(temp_output_dir):
    """Test job execution with wait_for_completion=True."""
    # Create test job scripts
    job_script_paths = []
    for i in range(3):
        path = os.path.join(temp_output_dir, f"test_job_{i}.sh")
        with open(path, "w") as f:
            f.write(f"#!/bin/bash\necho 'Test job {i}'\n")
        os.chmod(path, 0o755)
        job_script_paths.append(path)
    
    # First mock for submit_job
    def mock_side_effect(*args, **kwargs):
        if args[0][0] == "sbatch":
            mock_process = MagicMock()
            job_id = f"1234{args[0][1][-4]}"  # Use the job number from filename
            mock_process.stdout = f"Submitted batch job {job_id}"
            mock_process.stderr = ""
            return mock_process
        elif args[0][0] == "sacct":
            mock_process = MagicMock()
            job_id = args[0][2]
            mock_process.stdout = f"{job_id}|COMPLETED|0:0\n"
            mock_process.stderr = ""
            return mock_process
    
    with patch("subprocess.run", side_effect=mock_side_effect):
        with patch("time.sleep", return_value=None):  # Skip sleep
            pipe = BatchJobExecutorPipe()
            data = {
                "job_scripts": job_script_paths,
                "max_concurrent": 2,
                "wait_for_completion": True,
                "poll_interval": 1
            }
            
            result = pipe.execute(data)
            
            # Check outputs
            assert "job_ids" in result
            assert len(result["job_ids"]) == 3
            assert "completed_jobs" in result
            assert len(result["completed_jobs"]) == 3
            assert all(status == "COMPLETED" for status in result["completed_jobs"].values())

# --- FlowSplitJoinPipe with JobExecutorPipe Tests ---

def test_parallel_job_execution(temp_output_dir):
    """Test using FlowSplotJoinPipe with JobExecutorPipe for parallel job execution."""
    # Create multiple job scripts
    job_script_paths = []
    for i in range(3):
        path = os.path.join(temp_output_dir, f"test_job_{i}.sh")
        with open(path, "w") as f:
            f.write(f"#!/bin/bash\necho 'Test job {i}'\n")
        os.chmod(path, 0o755)
        job_script_paths.append(path)
    
    # Mock for job submission and status checking
    def mock_side_effect(*args, **kwargs):
        if args[0][0] == "sbatch":
            mock_process = MagicMock()
            script_name = os.path.basename(args[0][1])
            job_id = f"1000{script_name[-4]}"  # Use the job number from filename
            mock_process.stdout = f"Submitted batch job {job_id}"
            mock_process.stderr = ""
            return mock_process
        elif args[0][0] == "sacct":
            mock_process = MagicMock()
            job_id = args[0][2]
            mock_process.stdout = f"{job_id}|COMPLETED|0:0\n"
            mock_process.stderr = ""
            return mock_process
    
    # Setup the test with mocking
    with patch("subprocess.run", side_effect=mock_side_effect):
        with patch("time.sleep", return_value=None):  # Skip sleep
            # Create a single job executor pipe
            job_executor = JobExecutorPipe(wait_for_completion=True)
            
            # Create a parallel job executor
            parallel_job_executor = FlowSplitJoinPipe(
                inner_pipe=job_executor,
                input_mapping={"job_script": "i"},
                max_parallel=2  # Process at most 2 jobs in parallel
            )
            
            # Execute with the list of job scripts
            result = parallel_job_executor.execute({
                "job_script": job_script_paths
            })
            
            # Check the results
            assert "status" in result
            assert len(result["status"]) == 3
            
            for status in result["status"]:
                assert status == "COMPLETED"

def test_parallel_job_execution_integration(test_fasta_files, temp_output_dir):
    """Test integration of MEME command generation, SLURM job script creation, and parallel execution."""
    test_path, bg_path = test_fasta_files
    
    # Create a FlowSplitJoinPipe for MEME command generation
    meme_pipe = MemeCommandGeneratorPipe(output_dir_prefix=temp_output_dir)
    
    # Create test data for three different runs
    test_data = [
        {
            "summary_record": {
                "test_fasta_path": test_path,
                "background_fasta_path": bg_path,
                "run_id": f"test_run_{i}"
            }
        }
        for i in range(3)
    ]
    
    # Process each test configuration and collect MEME results
    meme_results = []
    for data in test_data:
        meme_result = meme_pipe.execute(data)
        meme_results.append(meme_result)
    
    # Create SLURM job scripts using the individual results
    slurm_pipe = SlurmJobGeneratorPipe(module_name="meme-5.4.1", job_name="test_job")
    job_scripts = []
    
    for meme_result in meme_results:
        slurm_data = {
            "command": meme_result["command"],
            "output_dir": meme_result["output_dir"]
        }
        slurm_result = slurm_pipe.execute(slurm_data)
        job_scripts.append(slurm_result["job_script"])
    
    # Mock for job submission and status checking
    def mock_side_effect(*args, **kwargs):
        if args[0][0] == "sbatch":
            mock_process = MagicMock()
            script_path = args[0][1]
            for i, result in enumerate(meme_results):
                if result["output_dir"] in script_path:
                    job_id = f"2000{i}"
                    break
            else:
                job_id = "20000"  # Default if not found
            mock_process.stdout = f"Submitted batch job {job_id}"
            mock_process.stderr = ""
            return mock_process
        elif args[0][0] == "sacct":
            mock_process = MagicMock()
            job_id = args[0][2]
            mock_process.stdout = f"{job_id}|COMPLETED|0:0\n"
            mock_process.stderr = ""
            return mock_process
    
    # Setup the test with mocking
    with patch("subprocess.run", side_effect=mock_side_effect):
        with patch("time.sleep", return_value=None):  # Skip sleep
            # Create a job executor pipe
            job_executor = JobExecutorPipe(wait_for_completion=True)
            
            # Create a parallel job executor
            parallel_job_executor = FlowSplitJoinPipe(
                inner_pipe=job_executor,
                input_mapping={"job_script": "i"},
                max_parallel=2
            )
            
            # Run the parallel job executor
            result = parallel_job_executor.execute({
                "job_script": job_scripts
            })
            
            # Check results
            assert "status" in result
            assert len(result["status"]) == 3
            
            # Verify all jobs completed successfully
            for status in result["status"]:
                assert status == "COMPLETED"