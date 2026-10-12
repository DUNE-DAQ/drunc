"""Regression tests for integration-test output extraction."""

import pytest
from rich.console import Console
from rich.table import Table

from drunc.integtest import integ_test_utils as utils


def _echo(marker: str) -> str:
    return f"[2026/03/17 10:48:15 UTC] INFO drunc.echo {marker}"


def _table(title: str, cells: list[str]) -> list[str]:
    table = Table(title=title)
    for index in range(len(cells)):
        table.add_column(f"column {index}")
    table.add_row(*cells)
    console = Console(width=240, color_system=None)
    with console.capture() as capture:
        console.print(table)
    return capture.get().splitlines()


@pytest.mark.parametrize("width", [6, 7])
def test_valid_process_table(width: int) -> None:
    """Accept both supported short process-table layouts."""
    cells = ["minimal", "mlt", "daq", "localhost", "uuid", "Alive", "0"][:width]
    lines = [_echo("before"), *_table("Processes running", cells), _echo("after")]
    rows = utils.get_ps_table_after_echo(lines, "before")
    assert rows == [dict(zip(utils.get_table_columns("ps")[:width], cells))]


@pytest.mark.parametrize("width", [0, 1, 5, 8])
def test_invalid_process_table_width(width: int) -> None:
    """Reject unsupported column counts instead of truncating a schema."""
    row = "\u2502 " + " \u2502 ".join(["cell"] * width) + " \u2502"
    lines = ["Processes running", row, "\u2514"]
    with pytest.raises(AssertionError, match="column"):
        utils._parse_table_from_index(lines, 0, "ps")


@pytest.mark.parametrize("width", [5, 7])
def test_inconsistent_process_table_width(width: int) -> None:
    """Reject later rows with a different width from the first row."""
    rows = [
        "\u2502 " + " \u2502 ".join(["cell"] * size) + " \u2502" for size in (6, width)
    ]
    with pytest.raises(AssertionError, match="column"):
        utils._parse_table_from_index(["Processes running", *rows, "\u2514"], 0, "ps")


def test_missing_table_border() -> None:
    """Reject truncated tables even when their data rows are valid."""
    lines = _table("Processes running", ["cell"] * 6)[:-1]
    with pytest.raises(AssertionError, match="closing border"):
        utils._parse_table_from_index(lines, 0, "ps")


def test_missing_table_does_not_find_later_command() -> None:
    """Do not borrow a table from a subsequent command."""
    lines = [
        _echo("before"),
        _echo("after"),
        *_table("Processes running", ["cell"] * 6),
    ]
    with pytest.raises(AssertionError, match="before"):
        utils.get_ps_table_after_echo(lines, "before")


def test_missing_process_output_is_not_empty_listing() -> None:
    """Fail extraction when neither a listing nor explicit empty output exists."""
    with pytest.raises(AssertionError, match="before"):
        utils.get_ps_table_after_echo([_echo("before"), _echo("after")], "before")


@pytest.mark.parametrize(
    "message", ["No processes running", "No processes running in session minimal"]
)
def test_explicit_empty_process_listing(message: str) -> None:
    """Recognise the shell's explicit empty-listing messages."""
    lines = [
        _echo("before"),
        f"INFO drunc.process_manager.shell {message}",
        _echo("after"),
    ]
    assert utils.get_ps_table_after_echo(lines, "before") == []


def test_status_title_does_not_match_echo_or_log() -> None:
    """Only a status table title anchors status-table extraction."""
    cells = ["mlt", "", "ready", "idle", "No", "Yes", "endpoint"]
    lines = [
        _echo("before"),
        "INFO checking status",
        *_table("minimal status", cells),
        _echo("after"),
    ]
    assert utils.get_status_table_after_echo(lines, "before")[0]["name"] == "mlt"
    with pytest.raises(AssertionError):
        utils.get_status_table_after_echo(
            [_echo("before"), _echo("status_done")], "before"
        )


def test_exact_marker_identity() -> None:
    """Do not confuse marker prefixes or suffixes with the requested marker."""
    lines = [
        _echo("test_wait_done"),
        _echo("other_test_wait"),
        _echo("test_wait"),
        "payload",
        _echo("test_wait_done"),
    ]
    assert utils.require_echo_marker_index(lines, "test_wait") == 2
    assert utils.get_lines_between_markers(lines, "test_wait", "test_wait_done") == [
        "payload"
    ]


def test_repeated_marker_is_ambiguous() -> None:
    """Require callers to disambiguate repeated markers with a start index."""
    lines = [_echo("before"), _echo("after"), _echo("before"), _echo("after")]
    with pytest.raises(AssertionError, match="[Aa]mbiguous"):
        utils.require_echo_marker_index(lines, "before")
    assert utils.require_echo_marker_index(lines, "before", start_idx=2) == 2


def test_server_marker_identity() -> None:
    """Match complete marker tokens in manager-side log messages."""
    lines = [
        "INFO server test_wait_done",
        "INFO server test_wait",
        "payload",
        "INFO server test_wait_done",
    ]
    assert utils.get_lines_between_markers(lines, "test_wait", "test_wait_done") == [
        "payload"
    ]


def test_ansi_output() -> None:
    """Normalise ANSI styling before marker and table extraction."""
    lines = [
        f"\x1b[32m{line}\x1b[0m"
        for line in [_echo("before"), *_table("Processes running", ["cell"] * 6)]
    ]
    assert len(utils.get_ps_table_after_echo(lines, "before")) == 1


@pytest.mark.parametrize(
    "table_type,width",
    [("status", 6), ("status", 8), ("exec_report", 2), ("run_info", 3)],
)
def test_invalid_non_process_width(table_type: str, width: int) -> None:
    """Apply exact column validation to the other table formats too."""
    with pytest.raises(AssertionError, match="column"):
        utils._parse_table_from_index(_table("title", ["cell"] * width), 0, table_type)


def test_empty_table_is_not_a_process_listing() -> None:
    """A printed process table without data is not explicit empty output."""
    with pytest.raises(AssertionError, match="rows"):
        utils._parse_table_from_index(["Processes running", "\u2514"], 0, "ps")
