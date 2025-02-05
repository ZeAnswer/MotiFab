import sys
import pytest
from src.cli import main

def test_cli_with_motif_string(monkeypatch, capsys):
    """
    Test that the CLI correctly parses parameters when a motif string is provided.
    Uses --dry-run so no actual file I/O is attempted.
    """
    test_args = [
        "cli.py",                       # Simulated script name
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--search-size", "100",
        "--injection-rate", "10%",      # Using percentage format
        "--background-mode", "select",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()  # Expect dry-run to call sys.exit(0)
    assert e.value.code == 0
    captured = capsys.readouterr().out
    # Check that the parsed arguments are printed.
    assert "Parsed arguments:" in captured
    assert "dummy.fasta" in captured
    assert "ACGTACGT" in captured
    assert "100" in captured
    assert "10%" in captured
    assert "select" in captured
    assert "Dry run mode" in captured

def test_cli_with_motif_length(monkeypatch, capsys):
    """
    Test that the CLI correctly parses parameters when a motif length is provided.
    Uses --dry-run so no actual file I/O is attempted.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-length", "12",
        "--search-size", "150",
        "--injection-rate", "15",      # Using absolute number
        "--background-mode", "shuffle",
        "--shuffle-method", "di-pair",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    # Expect a normal dry-run exit.
    assert e.value.code == 0
    captured = capsys.readouterr().out
    # Verify that the expected parameters are printed.
    assert "Parsed arguments:" in captured
    assert "dummy.fasta" in captured
    # Depending on how parameters are printed, check for "12" and "150".
    assert "12" in captured
    assert "150" in captured
    assert "15" in captured
    assert "shuffle" in captured
    assert "di-pair" in captured
    assert "Dry run mode" in captured

def test_cli_default_values(monkeypatch, capsys):
    """
    Test that default values are used for parameters not provided by the user.
    Uses --dry-run so no actual file I/O is attempted.
    Specifically, background-size, background-mode, shuffle-method, and output file paths.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--search-size", "100",
        "--injection-rate", "10%",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 0
    captured = capsys.readouterr().out
    # Check that the printed output includes default values.
    assert "1000" in captured  # background-size default
    assert "select" in captured  # background-mode default
    assert "naive" in captured   # shuffle-method default
    assert "search_set.fasta" in captured
    assert "background_set.fasta" in captured

def test_cli_error_multiple_motif_options(monkeypatch, capsys):
    """
    Test that the CLI exits with an error if more than one motif option is provided.
    The mutually exclusive group should force a single motif option.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--motif-length", "12",         # Conflict: two motif options provided.
        "--search-size", "100",
        "--injection-rate", "10%",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    # Expect non-zero exit code.
    assert e.value.code != 0

def test_cli_error_no_motif_option(monkeypatch, capsys):
    """
    Test that the CLI exits with an error if no motif option is provided.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--search-size", "100",
        "--injection-rate", "10%",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code != 0

def test_cli_error_missing_search_size(monkeypatch, capsys):
    """
    Test that the CLI exits with an error if the required search-size parameter is missing.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--injection-rate", "10%",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code != 0

def test_cli_error_missing_injection_rate(monkeypatch, capsys):
    """
    Test that the CLI exits with an error if the required injection-rate parameter is missing.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--search-size", "100",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code != 0