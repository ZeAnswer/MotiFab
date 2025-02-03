import sys
import pytest
from src.cli import main

def test_cli_with_motif_string(monkeypatch, capsys):
    """
    Test that the CLI correctly parses parameters when a motif string is provided.
    """
    test_args = [
        "cli.py",                       # Simulated script name
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--search-size", "100",
        "--injection-rate", "10%",      # Using percentage format
        "--background-mode", "select"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    main()  # Run the CLI main function

    captured = capsys.readouterr().out
    # Check that the parsed arguments are printed correctly.
    assert "Parsed arguments:" in captured
    assert "fasta: dummy.fasta" in captured or "dummy.fasta" in captured
    assert "motif_string: ACGTACGT" in captured or "ACGTACGT" in captured
    assert "search_size: 100" in captured or "100" in captured
    assert "injection_rate: 10%" in captured or "10%" in captured
    assert "background_mode: select" in captured or "select" in captured

def test_cli_with_motif_length(monkeypatch, capsys):
    """
    Test that the CLI correctly parses parameters when a motif length is provided.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-length", "12",
        "--search-size", "150",
        "--injection-rate", "15",      # Using absolute number
        "--background-mode", "shuffle",
        "--shuffle-method", "di-pair"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    main()

    captured = capsys.readouterr().out
    # Verify that the expected parameters are printed.
    assert "Parsed arguments:" in captured
    assert "fasta: dummy.fasta" in captured or "dummy.fasta" in captured
    assert "motif_length: 12" in captured or "12" in captured
    assert "search_size: 150" in captured or "150" in captured
    assert "injection_rate: 15" in captured or "15" in captured
    assert "background_mode: shuffle" in captured or "shuffle" in captured
    assert "shuffle_method: di-pair" in captured or "di-pair" in captured

def test_cli_default_values(monkeypatch, capsys):
    """
    Test that default values are used for parameters not provided by the user.
    Specifically, background-size, background-mode, shuffle-method, and output file paths.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--motif-string", "ACGTACGT",
        "--search-size", "100",
        "--injection-rate", "10%"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    main()

    captured = capsys.readouterr().out
    # Check for defaults:
    assert "background_size: 1000" in captured or "1000" in captured
    assert "background_mode: select" in captured or "select" in captured
    assert "shuffle_method: naive" in captured or "naive" in captured
    assert "output_search: search_set.fasta" in captured or "search_set.fasta" in captured
    assert "output_background: background_set.fasta" in captured or "background_set.fasta" in captured

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
        "--injection-rate", "10%"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    # Optionally, check that the exit code is non-zero.
    assert e.value.code != 0

def test_cli_error_no_motif_option(monkeypatch, capsys):
    """
    Test that the CLI exits with an error if no motif option is provided.
    """
    test_args = [
        "cli.py",
        "--fasta", "dummy.fasta",
        "--search-size", "100",
        "--injection-rate", "10%"
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
        "--injection-rate", "10%"
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
        "--search-size", "100"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code != 0