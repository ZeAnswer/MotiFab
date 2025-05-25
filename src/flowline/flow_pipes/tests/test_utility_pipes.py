import pytest
from flowline import UnitAmountConverterPipe, CommandExecutorPipe
import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# ------------------------------
# Tests for UnitAmountConverterPipe
# ------------------------------

def test_convert_amount():
    """Test converting an absolute amount."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3", "item4", "item5"]
    
    result = pipe.execute({"items": sample_items, "amount": 3})
    
    assert "amount" in result
    assert result["amount"] == 3

def test_convert_percentage_amount():
    """Test converting a percentage-based amount."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10"]
    
    result = pipe.execute({"items": sample_items, "amount": "50%"})
    
    assert "amount" in result
    assert result["amount"] == 5  # 50% of 10 items = 5

def test_convert_percentage_rounding():
    """Test that percentage conversion rounds to nearest integer."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3"]
    
    # 33.33% of 3 items = 0.9999, which should round to 1
    result = pipe.execute({"items": sample_items, "amount": "33.33%"})
    assert result["amount"] == 1
    
    # 66.67% of 3 items = 2.0001, which should round to 2
    result = pipe.execute({"items": sample_items, "amount": "66.67%"})
    assert result["amount"] == 2

def test_convert_empty_items_list():
    """Test converting with an empty items list."""
    pipe = UnitAmountConverterPipe()
    sample_items = []
    
    # Absolute amount with empty list
    result = pipe.execute({"items": sample_items, "amount": 5})
    assert result["amount"] == 0  # Should return 0 when no items exist
    
    # Percentage amount with empty list
    result = pipe.execute({"items": sample_items, "amount": "50%"})
    assert result["amount"] == 0  # Should return 0 when no items exist

def test_convert_amount_more_than_available():
    """Test that the result is limited to the number of available items."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3"]
    
    result = pipe.execute({"items": sample_items, "amount": 10})
    assert result["amount"] == 3  # Should be limited to the number of items
    
    result = pipe.execute({"items": sample_items, "amount": "200%"})
    assert result["amount"] == 3  # Should be limited to the number of items

def test_convert_invalid_percentage_format():
    """Test with invalid percentage format."""
    pipe = UnitAmountConverterPipe()
    sample_items = ["item1", "item2", "item3"]
    
    with pytest.raises(ValueError):
        pipe.execute({"items": sample_items, "amount": "invalid%"})
    
    with pytest.raises(ValueError):
        pipe.execute({"items": sample_items, "amount": "%50"})

def test_command_executor_success(tmp_path):
    """Test that a successful command execution returns COMPLETED status."""
    output_dir = str(tmp_path / "logs")
    command = "echo 'Hello, World'"
    pipe = CommandExecutorPipe(log_prefix="test_success")
    data = {"command": command, "output_dir": output_dir}
    result = pipe.execute(data)
    
    # Check that status is COMPLETED and exit_code is 0
    assert result["status"] == "COMPLETED"
    assert result["exit_code"] == 0
    
    # Check that the log file was created and contains expected content
    log_file = result["log_file"]
    assert os.path.exists(log_file)
    with open(log_file, "r") as f:
        content = f.read()
    assert "Command:" in content
    assert command in content
    assert "Hello, World" in content

def test_command_executor_failure(tmp_path):
    """Test that a command returning non-zero exit code returns FAILED status."""
    output_dir = str(tmp_path / "logs")
    # 'false' returns a nonzero exit code in Unix environments.
    command = "false"
    pipe = CommandExecutorPipe(log_prefix="test_failure")
    data = {"command": command, "output_dir": output_dir}
    result = pipe.execute(data)
    
    # Check that status is FAILED and exit_code is nonzero
    assert result["status"] == "FAILED"
    assert result["exit_code"] != 0
    assert os.path.exists(result["log_file"])

def test_command_executor_missing_command(tmp_path):
    """Test that missing 'command' input raises ValueError."""
    output_dir = str(tmp_path / "logs")
    pipe = CommandExecutorPipe()
    with pytest.raises(ValueError, match="Missing required input: command"):
        pipe.execute({"output_dir": output_dir})

def test_command_executor_missing_output_dir():
    """Test that missing 'output_dir' input raises ValueError."""
    pipe = CommandExecutorPipe()
    with pytest.raises(ValueError, match="Missing required input: output_dir"):
        pipe.execute({"command": "echo test"})

def test_command_executor_exception(tmp_path):
    """Test that an exception during command execution sets status to ERROR."""
    output_dir = str(tmp_path / "logs")
    command = "echo 'This should not run'"
    pipe = CommandExecutorPipe(log_prefix="test_exception")
    data = {"command": command, "output_dir": output_dir}
    
    # Patch subprocess.Popen to simulate an exception
    with patch("subprocess.Popen", side_effect=Exception("Simulated failure")):
        result = pipe.execute(data)
    
    assert result["status"] == "ERROR"
    assert "error_message" in result
    assert "Simulated failure" in result["error_message"]
    # Ensure the log file was created and contains the exception message
    assert os.path.exists(result["log_file"])
    with open(result["log_file"], "r") as f:
        log_content = f.read()
    assert "Exception occurred: Simulated failure" in log_content

def test_command_executor_str():
    """Test the string representation of the pipe."""
    pipe = CommandExecutorPipe()
    s = str(pipe)
    assert "CommandExecutorPipe" in s
    assert "Executing command" in s
