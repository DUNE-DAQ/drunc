import pathlib
import tempfile

import pytest
from click.exceptions import MissingParameter

from drunc.apps.process_wrapper import main as process_wrapper_main


@pytest.fixture(scope="function")
def tmp_path():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield pathlib.Path(tmpdirname)


def test_process_wrapper_success(tmp_path):
    """
    Test that the process wrapper correctly runs a successful command and logs output.
    Validates that the command's output is captured in the log file and that the return
    code is 0.
    Args:
        tmp_path: A temporary directory path provided by pytest fixture.
    """
    log_file = tmp_path / "process.log"
    cmd = 'echo "Test Success"'
    result = process_wrapper_main.main(
        args=["--log", str(log_file), cmd], standalone_mode=False
    )
    assert result == 0
    with open(log_file) as f:
        log_content = f.read()
    assert "Test Success" in log_content


def test_process_wrapper_failure(tmp_path):
    """
    Test that the process wrapper correctly runs a failing command and logs output.
    Validates that the command's output is captured in the log file and that the return
    code is non-zero.
    Args:
        tmp_path: A temporary directory path provided by pytest fixture.
    """
    log_file = tmp_path / "process.log"
    cmd = "exit 42"
    result = process_wrapper_main.main(
        args=["--log", str(log_file), cmd], standalone_mode=False
    )
    assert result == 42
    with open(log_file) as f:
        log_content = f.read()
    assert log_content == "" or log_content.isspace()


def test_process_wrapper_no_log(tmp_path):
    """
    Test that the process wrapper raises a MissingParameter error when no log file is specified.
    Args:
        tmp_path: A temporary directory path provided by pytest fixture.
    """
    cmd = 'echo "No log file"'
    with pytest.raises(MissingParameter):
        process_wrapper_main.main(args=[cmd], standalone_mode=False)


def test_process_wrapper_invalid_command(tmp_path):
    """
    Test that the process wrapper handles an invalid command gracefully.
    Validates that the command's failure is captured in the log file and that the return
    code is non-zero.
    Args:
        tmp_path: A temporary directory path provided by pytest fixture.
    """
    log_file = tmp_path / "process.log"
    cmd = "nonexistent_command_123"
    result = process_wrapper_main.main(
        args=["--log", str(log_file), cmd], standalone_mode=False
    )
    assert result != 0
    with open(log_file) as f:
        log_content = f.read()
    assert (
        "not found" in log_content
        or "No such file" in log_content
        or "command not found" in log_content
    )


def test_process_wrapper_logs_stderr(tmp_path):
    """
    Test that the process wrapper captures stderr output in the log file.
    Validates that stderr output from the command is present in the log file and that the
    return code is 0.
    Args:
        tmp_path: A temporary directory path provided by pytest fixture.
    """
    log_file = tmp_path / "process.log"
    cmd = "python -c \"import sys; sys.stderr.write('error\\n')\""
    result = process_wrapper_main.main(
        args=["--log", str(log_file), cmd], standalone_mode=False
    )
    assert result == 0
    with open(log_file) as f:
        log_content = f.read()
    assert "error" in log_content


def test_process_wrapper_multiple_commands(tmp_path):
    """
    Test that the process wrapper correctly runs multiple commands and logs output.
    Validates that the output from all commands is captured in the log file and that the
    return code is 0.
    Args:
        tmp_path: A temporary directory path provided by pytest fixture.
    """
    log_file = tmp_path / "process.log"
    cmd = 'echo "first" && echo "second"'
    result = process_wrapper_main.main(
        args=["--log", str(log_file), cmd], standalone_mode=False
    )
    assert result == 0
    with open(log_file) as f:
        log_content = f.read()
    assert "first" in log_content
    assert "second" in log_content
