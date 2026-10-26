from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Generic, Literal, NotRequired, TypedDict, TypeVar, overload

import pytest


class TableKind(Enum):
    PS = "ps"
    STATUS = "status"
    EXEC_REPORT = "exec_report"
    RUN_INFO = "run_info"
    TERMINATED = "terminated"


class AppRole(Enum):
    CONTROLLER = "controller"
    APPLICATION = "application"


class ProcessRow(TypedDict):
    session: str
    friendly_name: str
    user: str
    host: str
    uuid: str
    status: str
    exit_code: NotRequired[str]


class StatusRow(TypedDict):
    name: str
    info: str
    state: str
    substate: str
    in_error: str
    included: str
    endpoint: str


class ExecutionReportRow(TypedDict):
    name: str
    command_execution: str
    fsm_transition: str


RowT = TypeVar("RowT", ProcessRow, StatusRow, ExecutionReportRow)


@dataclass(frozen=True, init=False)
class TableResult(Generic[RowT]):
    kind: TableKind
    rows: tuple[RowT, ...]
    source: str | None
    start_marker: str
    stop_at: str | None
    line_span: tuple[int, int]

    def __init__(
        self,
        kind: TableKind,
        rows: list[RowT],
        *,
        source: str | None,
        start_marker: str,
        stop_at: str | None,
        line_span: tuple[int, int],
    ):
        pass


@dataclass(frozen=True, init=False)
class RunInfoResult:
    kind: TableKind
    values: dict[str, str]
    source: str | None
    start_marker: str
    stop_at: str | None
    line_span: tuple[int, int]

    def __init__(
        self,
        values: dict[str, str],
        *,
        source: str | None,
        start_marker: str,
        stop_at: str | None,
        line_span: tuple[int, int],
    ):
        pass


class LogLines:
    lines: tuple[str, ...]
    source: str | None

    def __init__(
        self, lines: list[str] | tuple[str, ...], *, source: str | None = None
    ):
        pass

    @classmethod
    def from_stdout(cls, stdout: str, *, source: str | None = None) -> LogLines:
        pass

    def require(
        self,
        *,
        text: str | None = None,
        pattern: re.Pattern[str] | None = None,
        echo: str | None = None,
        start_idx: int = 0,
        error_message: str | None = None,
    ) -> tuple[int, re.Match[str] | None]:
        pass

    def between(self, start, end, *, echo=True, start_idx=0):
        pass

    def assert_contains_between(
        self, start, end, expected_text, *, echo=False, start_idx=0
    ):
        pass

    @staticmethod
    def _strip_ansi(text: str) -> str:
        pass

    def _find(self, predicate, *, start_idx=0):
        pass

    def _validate_selector(self, *, text, pattern, echo):
        pass

    def _matches_marker(self, line, marker, *, echo=True):
        pass


class TableReader:
    def __init__(self, output):
        pass

    @overload
    def after_echo(
        self,
        marker: str,
        kind: Literal[TableKind.PS, TableKind.TERMINATED],
        *,
        stop_at: str | None = None,
    ) -> TableResult[ProcessRow]:
        pass

    @overload
    def after_echo(
        self,
        marker: str,
        kind: Literal[TableKind.STATUS],
        *,
        stop_at: str | None = None,
    ) -> TableResult[StatusRow]:
        pass

    @overload
    def after_echo(
        self,
        marker: str,
        kind: Literal[TableKind.EXEC_REPORT],
        *,
        stop_at: str | None = None,
    ) -> TableResult[ExecutionReportRow]:
        pass

    @overload
    def after_echo(
        self,
        marker: str,
        kind: Literal[TableKind.RUN_INFO],
        *,
        stop_at: str | None = None,
    ) -> RunInfoResult:
        pass

    @overload
    def after_echo(
        self,
        marker: str,
        kind: TableKind,
        *,
        stop_at: str | None = None,
    ) -> (
        TableResult[ProcessRow]
        | TableResult[StatusRow]
        | TableResult[ExecutionReportRow]
        | RunInfoResult
    ):
        pass

    def after_echo(
        self,
        marker: str,
        kind: TableKind,
        *,
        stop_at: str | None = None,
    ) -> (
        TableResult[ProcessRow]
        | TableResult[StatusRow]
        | TableResult[ExecutionReportRow]
        | RunInfoResult
    ):
        pass

    def rows(
        self,
        table: TableResult[RowT],
        *,
        name: str,
        session: str | None = None,
    ) -> list[RowT]:
        pass

    def value(
        self,
        table: TableResult[RowT],
        *,
        name: str,
        column: str,
        session: str | None = None,
    ) -> str:
        pass

    def assert_status(
        self,
        table: TableResult[StatusRow],
        *,
        state: str | None = None,
        roles: Mapping[str, AppRole] | None = None,
        substates: Mapping[AppRole, str] | None = None,
    ):
        pass

    def assert_uuids(self, table: TableResult[ProcessRow]):
        pass

    def assert_presence(
        self,
        table: TableResult[RowT],
        *,
        name,
        session=None,
        present=True,
        context="",
    ):
        pass

    def assert_execution_success(self, table: TableResult[ExecutionReportRow]):
        pass

    def _filter_rows(self, table, column, value):
        pass

    def _get_section(self, marker, *, stop_at=None):
        pass

    def _role_for_name(self, name: str, roles: Mapping[str, AppRole]) -> AppRole:
        pass

    def _get_table_spec(self, kind):
        pass

    def _find_table(self, lines, spec):
        pass

    def _validate_header(self, headers, spec):
        pass

    def _parse_table(self, lines, start_idx, spec):
        pass


def require_pattern_match(text, pattern, *, error_message):
    pass


def check_file_containing(lines, file):
    pass


def assert_match_contains_uuid(text, pattern, *, error_message):
    pass


########################################################################################


def test_dunerc_success(run_dunerc, caplog) -> None:
    """Existing infrastructure checks omitted from this API usage sketch."""
    pass


def test_log_files(run_dunerc) -> None:
    """Existing logfile existence/error checks omitted from this API usage sketch."""
    pass


@pytest.fixture(scope="module")
def output(run_dunerc):
    return LogLines.from_stdout(
        run_dunerc.completed_processes["drunc"].stdout, source="drunc"
    )


@pytest.fixture(scope="module")
def tables(output):
    return TableReader(output)


def test_boot(output, tables):
    output.assert_contains_between(
        "pre_boot", "post_boot", "No processes running in session"
    )
    processes = tables.after_echo("post_boot", TableKind.PS, stop_at="test_logs")
    assert processes.rows
    tables.assert_uuids(processes)


def test_unknown_log_command(output):
    output.require(
        text="Bad query for logs: The process corresponding to the query doesn't exist"
    )


def test_root_controller_logs(output):
    header_idx, _ = output.require(text="root-controller logs")
    footer_idx, _ = output.require(text="root-controller end", start_idx=header_idx + 1)
    lines = output.lines[header_idx + 1 : footer_idx]
    assert len(lines) == 5
    require_pattern_match(
        "\n".join(lines),
        re.compile(
            r"drunc\.controller\.core\.init_controller.*Controller ready\s*$",
            re.MULTILINE,
        ),
        error_message="Controller not ready",
    )


@pytest.mark.parametrize(
    "marker,seconds,steps", [("test_wait", 10, None), ("test_wait_progress", 5, 10)]
)
def test_wait(output, marker, seconds, steps):
    output.require(echo=marker)
    lines = output.between(marker, f"{marker}_done")
    section = LogLines(lines, source="drunc")
    timestamp = r"\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) UTC\]"
    start_idx, start = section.require(
        pattern=re.compile(
            timestamp
            + r".*Command wait running for (\d+) seconds(?: with (\d+) steps printed)?"
        )
    )
    end_idx, end = section.require(
        pattern=re.compile(timestamp + r".*Command wait ran for (\d+) seconds\."),
        start_idx=start_idx + 1,
    )
    assert start is not None and end is not None
    assert int(start.group(2)) == int(end.group(2)) == seconds
    elapsed = datetime.strptime(end.group(1), "%Y/%m/%d %H:%M:%S") - datetime.strptime(
        start.group(1), "%Y/%m/%d %H:%M:%S"
    )
    assert abs(elapsed.total_seconds() - seconds) <= 1
    if steps is not None:
        assert int(start.group(3)) == steps
        progress = lines[start_idx + 1 : end_idx]
        assert len(progress) == steps
        for step, line in enumerate(progress, start=1):
            match = require_pattern_match(
                line,
                re.compile(r"Progress: (\d+(?:\.\d+)?) / (\d+) seconds"),
                error_message=f"Invalid progress step {step}",
            )
            assert abs(float(match.group(1)) - step * seconds / steps) < 1e-6
            assert int(match.group(2)) == seconds


def test_restart_mlt_logs(output):
    text = "\n".join(output.between("pre_restart_mlt", "post_restart_mlt"))
    require_pattern_match(
        text,
        re.compile(
            r"Remote process .*?terminated gracefully following SIGQUIT signal\.",
            re.DOTALL,
        ),
        error_message="Missing graceful termination",
    )
    require_pattern_match(
        text,
        re.compile(
            r"Process 'mlt' \(.*?\) was terminated by the process manager through the remote pid\. Reported exit code: 0\.",
            re.DOTALL,
        ),
        error_message="Missing mlt exit code",
    )
    assert_match_contains_uuid(
        text,
        re.compile(
            r"Restarted \['mlt'\] from session \S+ with UUID\s+([^\s\n]+)", re.DOTALL
        ),
        error_message="Missing restart UUID",
    )


@pytest.mark.parametrize(
    "marker,stop_at,present",
    [
        ("test_kill_mlt", "test_kill_mlt_post", True),
        ("test_kill_mlt_post", "test_kill_mlt_done", False),
        ("test_recovery_post", "test_recovery_done", True),
    ],
)
def test_mlt_presence(tables, marker, stop_at, present):
    processes = tables.after_echo(marker, TableKind.PS, stop_at=stop_at)
    tables.assert_presence(processes, name="mlt", present=present, context=marker)


def test_flush(tables):
    initial = tables.after_echo("test_flush", TableKind.PS, stop_at="after_crash")
    tables.assert_presence(initial, name="mlt")
    crashed = tables.after_echo("after_crash", TableKind.PS, stop_at="after_flush")
    assert tables.value(crashed, name="mlt", column="status") == "Dead"
    flushed = tables.after_echo("after_flush", TableKind.PS, stop_at="test_flush_done")
    tables.assert_presence(flushed, name="mlt", present=False)


def example_fsm_usage(
    tables,
    boot_status_table: TableResult[StatusRow],
    roles: Mapping[str, AppRole],
    params,
):
    """Controller check preserving the current _check_command assertions."""
    report = tables.after_echo(
        params.marker, TableKind.EXEC_REPORT, stop_at=params.done_marker
    )
    assert report.rows
    boot_names = {row["name"] for row in boot_status_table.rows}
    report_names = {row["name"] for row in report.rows}
    assert report_names == boot_names, (
        f"{report.source}, {report.start_marker}, lines {report.line_span}: "
        f"missing {boot_names - report_names}; unexpected {report_names - boot_names}"
    )
    tables.assert_execution_success(report)
    status = tables.after_echo(
        params.done_marker, TableKind.STATUS, stop_at=f"{params.marker}_status_done"
    )
    assert status.rows
    tables.assert_status(
        status,
        state=params.expected_state,
        roles=roles,
        substates={
            AppRole.CONTROLLER: params.expected_state,
            AppRole.APPLICATION: params.non_controller_substate,
        },
    )
    if params.run_number is not None:
        run_info = tables.after_echo(
            params.done_marker,
            TableKind.RUN_INFO,
            stop_at=f"{params.marker}_status_done",
        )
        assert run_info.values
        assert run_info.values["Run number"] == str(params.run_number)


def example_session_lookup(tables, processes: TableResult[ProcessRow]):
    """Given mlt in session-a (Alive) and session-b (Dead)."""
    assert len(tables.rows(processes, name="mlt")) == 2
    with pytest.raises(AssertionError):
        tables.value(processes, name="mlt", column="status")
    assert (
        tables.value(processes, name="mlt", session="session-b", column="status")
        == "Dead"
    )
    tables.assert_presence(processes, name="mlt", session="session-a")


def example_file_check(logfile):
    assert check_file_containing(["Controller ready"], logfile)
