"""Shared helpers for drunc integration tests.

This module centralizes commoon patterns used by process-manager integration tests. 
Importantly, most of these are defined to help with processing the stdout log outputs
of the integ tests. 

Common functions include:
- searching ordered log output for marker lines,
- requiring regex/string matches with informative assertion errors,
- extracting process-table rows from `ps` command output,
- asserting process presence/absence by friendly name.

The helpers are intentionally lightweight and pytest-friendly: failures are
reported through `assert` with context-rich messages.
"""

import re
from collections.abc import Callable

ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-9;]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from a text block."""
    return ANSI_ESCAPE_RE.sub("", text)


def find_line_index(
    lines: list[str],
    predicate: Callable[[str], bool],
    *,
    start_idx: int = 0,
) -> int | None:
    """Return the first line index at or after `start_idx` matching `predicate`.

    Returns `None` when no line matches.
    """
    return next(
        (idx for idx in range(start_idx, len(lines)) if predicate(lines[idx])),
        None,
    )


def require_line_index(
    lines: list[str],
    predicate: Callable[[str], bool],
    *,
    error_message: str,
    start_idx: int = 0,
) -> int:
    """Like `find_line_index`, but assert a match exists and return its index."""
    line_idx = find_line_index(lines, predicate, start_idx=start_idx)
    assert line_idx is not None, error_message
    return line_idx


def require_line_containing(
    lines: list[str],
    text: str,
    *,
    error_message: str,
    start_idx: int = 0,
) -> int:
    """Assert and return index of the first line containing `text`."""
    return require_line_index(
        lines,
        lambda line: text in line,
        error_message=error_message,
        start_idx=start_idx,
    )


def require_echo_marker_index(
    lines: list[str], echo_marker: str, *, start_idx: int = 0
) -> int:
    """Assert and return index of a `drunc.echo` line ending with `echo_marker`.
    This is hardcoded since echo is a specific callable function with its own logger.
    """
    return require_line_index(
        lines,
        lambda line: "drunc.echo" in line and line.rstrip().endswith(echo_marker),
        error_message=(f"Could not find drunc.echo marker '{echo_marker}' in stdout."),
        start_idx=start_idx,
    )


def require_pattern_match_index(
    lines: list[str],
    pattern: re.Pattern[str],
    *,
    error_message: str,
    start_idx: int = 0,
) -> tuple[int, re.Match[str]]:
    """Assert and return `(index, match)` for first line matching `pattern`."""
    line_idx = require_line_index(
        lines,
        lambda line: pattern.search(line) is not None,
        error_message=error_message,
        start_idx=start_idx,
    )
    match = pattern.search(lines[line_idx])
    assert match is not None
    return line_idx, match


def require_pattern_match(
    text: str,
    pattern: re.Pattern[str],
    *,
    error_message: str,
) -> re.Match[str]:
    """Assert `pattern` matches `text` and return the `re.Match` object."""
    match = pattern.search(text)
    assert match is not None, error_message
    return match


def _parse_ps_table_from_index(
    lines: list[str], start_idx: int
) -> list[dict[str, str]]:
    """Parse a Unicode table of processes starting after `start_idx`.

    The parser expects rows that start with `│` and stops at a line starting
    with `└`. It returns dictionaries with normalized column names.
    """
    table_rows: list[dict[str, str]] = []

    for line in lines[start_idx + 1 :]:
        stripped = line.strip()

        if stripped.startswith("└"):
            break

        if not stripped.startswith("│"):
            continue

        cells = [cell.strip() for cell in stripped.strip("│").split("│")]
        if len(cells) < 7:
            continue

        table_rows.append(
            {
                "session": cells[0],
                "friendly_name": cells[1],
                "user": cells[2],
                "host": cells[3],
                "uuid": cells[4],
                "alive": cells[5],
                "exit_code": cells[6],
            }
        )

    return table_rows


def get_ps_table_after_echo(stdout: str, echo_marker: str) -> list[dict[str, str]]:
    """Return parsed process-table rows found after a specific echo marker.

    If no process table is found after the marker, returns an empty list.
    """
    lines = strip_ansi(stdout).splitlines()

    echo_idx = require_echo_marker_index(lines, echo_marker)

    table_start_idx = find_line_index(
        lines,
        lambda line: "Processes running" in line,
        start_idx=echo_idx + 1,
    )
    if table_start_idx is None:
        return []

    return _parse_ps_table_from_index(lines, table_start_idx)


def get_uuid_for_friendly_name(
    ps_table: list[dict[str, str]], friendly_name: str
) -> str:
    """Return UUID for `friendly_name` from a parsed process table.

    Raises:
        AssertionError: if the friendly name is absent.
    """
    for row in ps_table:
        if row["friendly_name"].strip() == friendly_name:
            return row["uuid"]

    available_names = ", ".join(row["friendly_name"].strip() for row in ps_table)
    raise AssertionError(
        f"Could not find friendly name '{friendly_name}' in ps table. "
        f"Available names: {available_names}"
    )


def get_rows_for_friendly_name(
    ps_table: list[dict[str, str]], friendly_name: str
) -> list[dict[str, str]]:
    """Return all rows whose `friendly_name` matches exactly after stripping."""
    return [row for row in ps_table if row["friendly_name"].strip() == friendly_name]


def assert_process_presence(
    ps_table: list[dict[str, str]],
    friendly_name: str,
    *,
    expected_present: bool,
    context: str,
) -> None:
    """Assert whether a process is present/absent in a process table.

    Args:
        ps_table: Parsed process rows.
        friendly_name: Process name to check.
        expected_present: `True` if process should exist, `False` otherwise.
        context: Short phrase appended to error text (e.g. "before kill").
    """
    matching_rows = get_rows_for_friendly_name(ps_table, friendly_name)

    if expected_present:
        assert matching_rows, (
            f"Expected to find '{friendly_name}' in ps table {context}, but it was missing."
        )
        return

    assert not matching_rows, (
        f"Expected '{friendly_name}' to be absent from ps table {context}, but it is still present."
    )


def assert_process(
    ps_table: list[dict[str, str]],
    friendly_name: str,
    *,
    context: str,
    expected_present: bool = True,
) -> None:
    """Convenience wrapper around `assert_process_presence`.

    By default, asserts that the process is present.
    """
    assert_process_presence(
        ps_table,
        friendly_name,
        expected_present=expected_present,
        context=context,
    )