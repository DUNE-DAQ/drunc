"""Shared helpers for drunc integration tests.

This module centralizes common patterns used by process-manager integration tests.
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

    Example:
        >>> lines = [
        ...     "[2026/03/17 10:48:10 UTC] INFO drunc.controller.iface Command wait running for 5 seconds.",
        ...     "[2026/03/17 10:48:15 UTC] INFO drunc.controller.iface Command wait ran for 5 seconds.",
        ...     "[2026/03/17 10:48:15 UTC] INFO drunc.echo test_recovery_post",
        ... ]
        >>> find_line_index(lines, lambda line: "Command wait ran" in line)
        1
        >>> find_line_index(lines, lambda line: "test_wait_done" in line) is None
        True
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
    """Like `find_line_index`, but assert a match exists and return its index.

    Example:
        >>> lines = [
        ...     "[2026/03/17 10:47:38 UTC] INFO drunc.echo test_wait",
        ...     "[2026/03/17 10:47:48 UTC] INFO drunc.echo test_wait_done",
        ... ]
        >>> require_line_index(
        ...     lines,
        ...     lambda line: "test_wait_done" in line,
        ...     error_message="Could not find wait completion marker",
        ... )
        1
    """
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
    """Assert and return index of the first line containing `text`.

    Example:
    [2026/03/17] WARNING drunc.process_manager_driver Bad query for logs
    ────────────────────────────── root-controller logs ──────────────────────────────
    [2026/03/17] INFO drunc.init_controller Taking control of trg-controller

    header_idx = require_line_containing(
        lines,
        "root-controller logs",
        error_message="Did not find the 'root-controller logs' header line in stdout.",
    )


    """
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

    Example:
        >>> lines = [
        ...     "[2026/03/17 10:48:15 UTC] INFO drunc.echo test_recovery_post",
        ...     "Processes running",
        ... ]
        >>> require_echo_marker_index(lines, "test_recovery_post")
        0
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
    """Assert and return `(index, match)` for first line matching `pattern`.

    Example:
        >>> lines = [
        ...     "[2026/03/17] INFO drunc.iface Command wait running for 10 seconds.",
        ...     "[2026/03/17] INFO drunc.iface Command wait ran for 10 seconds.",
        ... ]
        >>> pattern = re.compile(r"Command wait ran for (\\d+) seconds\\.")
        >>> line_idx, match = require_pattern_match_index(
        ...     lines,
        ...     pattern,
        ...     error_message="Did not find wait completion log line.",
        ... )
        >>> (line_idx, match.group(1))
        (1, '10')
    """
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
    """Assert `pattern` matches `text` and return the `re.Match` object.

    Example:
        >>> line = "[2026/03/17] INFO Command wait ran for 10 seconds."
        >>> pattern = re.compile(r"Command wait ran for (\\d+) seconds\\.")
        >>> match = require_pattern_match(
        ...     line,
        ...     pattern,
        ...     error_message="Did not find wait completion log line.",
        ... )
        >>> match.group(1)
        '10'
    """
    match = pattern.search(text)
    assert match is not None, error_message
    return match


# ── Table parsing ──────────────────────────────────────────────────────────────


def _parse_table_from_index(
    lines: list[str], start_idx: int, columns: list[str]
) -> list[dict[str, str]]:
    """Parse a Unicode box table starting after `start_idx`, mapping cells to `columns`.

    Expects rows that start with `│` and stops at a line starting with `└`.
    Rows with fewer cells than `columns` are silently skipped.
    """
    rows: list[dict[str, str]] = []

    for line in lines[start_idx + 1 :]:
        stripped = line.strip()
        if stripped.startswith("└"):
            break
        if not stripped.startswith("│"):
            continue
        cells = [cell.strip() for cell in stripped.strip("│").split("│")]
        if len(cells) < len(columns):
            continue
        rows.append(dict(zip(columns, cells)))

    return rows


def _get_table_after_echo(
    lines: list[str],
    echo_marker: str,
    header_keyword: str,
    columns: list[str],
) -> list[dict[str, str]]:
    """Return parsed table rows found after `echo_marker`, anchored by `header_keyword`.

    Args:
        stdout:          Raw stdout string (ANSI stripping is handled internally).
        echo_marker:     The drunc.echo marker to anchor the search.
        header_keyword:  Substring identifying the table header line.
        columns:         Ordered column names to map to each cell.

    Returns:
        Parsed rows as a list of dicts. Empty list if no table is found.
    """
    echo_idx = require_echo_marker_index(lines, echo_marker)

    table_start_idx = find_line_index(
        lines,
        lambda line: header_keyword in line,
        start_idx=echo_idx + 1,
    )
    if table_start_idx is None:
        return []

    return _parse_table_from_index(lines, table_start_idx, columns)


_PS_COLUMNS = ["session", "friendly_name", "user", "host", "uuid", "alive", "exit_code"]
_STATUS_COLUMNS = [
    "name",
    "info",
    "state",
    "substate",
    "in_error",
    "included",
    "endpoint",
]
_EXEC_REPORT_COLUMNS = ["name", "command_execution", "fsm_transition"]


def get_ps_table_after_echo(lines: list[str], echo_marker: str) -> list[dict[str, str]]:
    """Return parsed process-table rows found after a specific echo marker.

    If no process table is found after the marker, returns an empty list.

    Example:
        >>> stdout = (
        ...     "[2026/03/17 10:48:15 UTC] INFO drunc.echo test_recovery_post\n"
        ...     "Processes running\n"
        ...     "│ minimal │ root-controller │ emmuhamm │ localhost │ f201f9c7-b910-4100-bd78-11765a4d2ee1 │ True │ 0 │\n"
        ...     "└"
        ... )
        >>> table = get_ps_table_after_echo(stdout, "test_recovery_post")
        >>> table[0]["friendly_name"]
        'root-controller'
    """
    return _get_table_after_echo(lines, echo_marker, "Processes running", _PS_COLUMNS)


def get_status_table_after_echo(
    lines: list[str], echo_marker: str
) -> list[dict[str, str]]:
    """Return parsed status-table rows found after a specific echo marker.

    If no status table is found after the marker, returns an empty list.

    Returns:
        Parsed rows with keys: name, info, state, substate, in_error, included, endpoint.
    """
    return _get_table_after_echo(lines, echo_marker, "status", _STATUS_COLUMNS)


def get_execution_report_after_echo(
    lines: list[str], echo_marker: str
) -> list[dict[str, str]]:
    """Return parsed execution-report rows found after a specific echo marker.

    If no execution report is found after the marker, returns an empty list.

    Returns:
        Parsed rows with keys: name, command_execution, fsm_transition.
    """
    return _get_table_after_echo(
        lines, echo_marker, "execution report", _EXEC_REPORT_COLUMNS
    )


# ── Process table helpers ──────────────────────────────────────────────────────


def get_column_for_friendly_name(
    ps_table: list[dict[str, str]], friendly_name: str, column: str
) -> str:
    """Return the column for `friendly_name` from a parsed process table.

    Raises:
        AssertionError: if the friendly name is absent.
    """
    for row in ps_table:
        if row["friendly_name"].strip() == friendly_name:
            return row[column]

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
    context: str,
    expected_present: bool = True,
) -> None:
    """Assert whether a process is present/absent in a process table.

    Args:
        ps_table: Parsed process rows.
        friendly_name: Process name to check.
        expected_present: `True` if process should exist, `False` otherwise.
        context: Short phrase appended to error text (e.g. "before kill").

    Example:
        >>> ps_table = [
        ...     {
        ...         "session": "minimal",
        ...         "friendly_name": "root-controller",
        ...         "user": "daq",
        ...         "host": "localhost",
        ...         "uuid": "f201f9c7-b910-4100-bd78-11765a4d2ee1",
        ...         "alive": "True",
        ...         "exit_code": "0",
        ...     }
        ... ]
        >>> assert_process_presence(
        ...     ps_table,
        ...     "root-controller",
        ...     context="before restart",
        ...     expected_present=True,
        ... )
        >>> assert_process_presence(
        ...     ps_table,
        ...     "mlt",
        ...     context="after restart",
        ...     expected_present=False,
        ... )
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


# ── Status table assertion helpers ────────────────────────────────────────────


def check_execution_report_success(report: list[dict[str, str]]) -> None:
    """Assert every row in an execution report shows success for both columns.

    Raises:
        AssertionError: On the first row that fails either check,
                        with the process name and actual values reported.
    """
    assert report, "Execution report is empty — nothing to check."

    for row in report:
        name = row["name"]
        assert row["command_execution"] == "Executed Successfully", (
            f"Process '{name}': expected command_execution='Executed Successfully', "
            f"got '{row['command_execution']}'."
        )
        assert row["fsm_transition"] == "Fsm Executed Successfully", (
            f"Process '{name}': expected fsm_transition='Fsm Executed Successfully', "
            f"got '{row['fsm_transition']}'."
        )


def check_status_table_states(
    status_table: list[dict[str, str]],
    expected_state: str,
) -> None:
    """Assert that every row in a status table has the expected `state`.

    Raises:
        AssertionError: Lists all rows whose state does not match.
    """
    assert status_table, "Status table is empty — nothing to check."

    failures = [
        f"  '{row['name']}': state='{row['state']}'"
        for row in status_table
        if row["state"] != expected_state
    ]
    assert not failures, (
        f"Expected all processes to have state='{expected_state}', "
        f"but the following did not:\n" + "\n".join(failures)
    )


def check_status_table_substates(
    status_table: list[dict[str, str]],
    controller_substate: str,
    non_controller_substate: str,
) -> None:
    """Assert substates based on whether a process name contains 'controller'.

    - Rows whose `name` contains 'controller' must match `controller_substate`.
    - All other rows must match `non_controller_substate`.

    Raises:
        AssertionError: Lists all rows whose substate does not match the rule.
    """
    assert status_table, "Status table is empty — nothing to check."

    failures: list[str] = []
    for row in status_table:
        name = row["name"]
        is_controller = "controller" in name
        expected = controller_substate if is_controller else non_controller_substate
        if row["substate"] != expected:
            failures.append(
                f"  '{name}': expected substate='{expected}', got '{row['substate']}'"
            )

    assert not failures, "Substate mismatch(es) found:\n" + "\n".join(failures)
