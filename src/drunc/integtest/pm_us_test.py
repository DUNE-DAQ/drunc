# The goal of this test is to test all functions in the unified shell, when connected
# to a standalone process manager shell
# Because the unified shell encompassees both the process manager shell and the
# controller shell, the contents of this test cover both the process_manager_test
# and the controller_test. However this is all done in the context of
# the split shell functionality.

import functools
import getpass
import os
import re
from datetime import datetime

import integrationtest.data_classes as idc
import integrationtest.log_file_checks as log_file_checks
import pytest
from daqconf.utils import find_free_port
from integ_test_utils import (
    _parse_table_from_index,
    assert_contains_between_markers,
    assert_match_contains_uuid,
    assert_process_presence,
    assert_rows_have_valid_uuids,
    check_execution_report_success,
    check_status_table_states,
    check_status_table_substates,
    find_line_index,
    get_column_for_friendly_name,
    get_execution_report_after_echo,
    get_lines_between_markers,
    get_ps_table_after_echo,
    get_run_info_after_echo,
    get_status_table_after_echo,
    get_text_between_echo_markers,
    require_echo_marker_index,
    require_line_containing,
    require_pattern_match,
    require_pattern_match_index,
    strip_ansi,
)
from pm_test_common import FsmCommandParams, ignored_logfile_problems, make_conf_dict

print = functools.partial(print, flush=True)  # always flush print() output

pytest_plugins = "integrationtest.integrationtest_drunc"

conf_dict = make_conf_dict("pm_us")

confgen_arguments = {"SmallFootprint": conf_dict}

# ── FSM command definitions (mirrors controller_test.py) ───────────────────────

_FSM_COMMANDS = [
    FsmCommandParams("test_conf", "conf", "configured"),
    FsmCommandParams(
        "test_start", "start", "ready", command_args=["--run-number", "1"], run_number=1
    ),
    FsmCommandParams("test_enable_triggers", "enable-triggers", "running"),
    FsmCommandParams("test_disable_triggers", "disable-triggers", "ready"),
    FsmCommandParams("test_drain_dataflow", "drain-dataflow", "dataflow_drained"),
    FsmCommandParams(
        "test_stop_trigger_sources", "stop-trigger-sources", "trigger_sources_stopped"
    ),
    FsmCommandParams("test_stop", "stop", "configured"),
    FsmCommandParams("test_scrap", "scrap", "initial"),
]

_FSM_SEQUENCES = {
    "test_start_run": FsmCommandParams(
        "test_start_run",
        "start-run",
        "running",
        command_args=["--run-number", "2"],
        run_number=2,
    ),
    "test_srun_w_boot": FsmCommandParams(
        "test_srun_w_boot",
        "start-run",
        "running",
        command_args=["--run-number", "5"],
        run_number=5,
    ),
    "test_srunw_boot_conf": FsmCommandParams(
        "test_srunw_boot_conf",
        "start-run",
        "running",
        command_args=["--run-number", "6"],
        run_number=6,
    ),
    "test_stop_run": FsmCommandParams("test_stop_run", "stop-run", "configured"),
}

_SHUTDOWN_MARKER = "test_shutdown"
_SHUTDOWN_STATUS_ERROR = (
    "Controller-specific commands cannot be sent until the session is booted"
)

# The commands to run in dunerc and the unified shell
dunerc_commands = (
    """

    echo pre_boot
    echo-on-server pre_boot
    ps -w 300
    boot
    wait 15
    echo post_boot
    echo-on-server post_boot
    ps -w 300


    echo test_logs
    logs --name unknown
    logs --name root-controller --how-far 5
    logs --name mlt --how-far 5
    echo test_logs_done

    echo test_wait
    wait 10
    echo test_wait_done

    echo pre_restart_mlt
    echo-on-server pre_restart_mlt
    restart -n mlt
    restart -n root-controller
    wait 5
    echo post_restart_mlt
    ps -w 300
    echo-on-server post_restart_mlt


    echo test_kill_mlt
    ps -w 300
    kill -n mlt
    wait 2
    echo test_kill_mlt_post
    ps -w 300
    echo test_kill_mlt_done


    echo test_recovery
    restart -n mlt
    restart -n trg-controller
    wait 5
    echo test_recovery_post
    ps -w 300
    echo test_recovery_done

    echo pre_fsm_status
    status -w 140
    echo pre_fsm_status_done

    echo test_flush
    ps -w 300
    kill -n mlt --crash
    wait 5
    echo after_crash
    ps -w 300
    flush
    echo after_flush
    ps -w 300
    echo test_flush_done

    echo test_terminate
    echo-on-server test_terminate
    terminate
    echo test_terminate_done
    echo-on-server test_terminate_done

    wait 15
    boot
    


    """
    + "".join(p.to_command_block() for p in _FSM_COMMANDS)
    + _FSM_SEQUENCES["test_srun_w_boot"].to_command_block()
    + " terminate "
    + "boot wait 10 conf"
    + _FSM_SEQUENCES["test_srunw_boot_conf"].to_command_block()
    + " terminate "
    + _FSM_SEQUENCES["test_start_run"].to_command_block()
    + _FSM_SEQUENCES["test_stop_run"].to_command_block()
    + "start-run --run-number 3"
    + _FSM_SEQUENCES["test_stop_run"].to_command_block()
    + f"""
    echo {_SHUTDOWN_MARKER}
    shutdown
    echo {_SHUTDOWN_MARKER}_done
    status -w 140
    echo {_SHUTDOWN_MARKER}_status_done
    """
).split()

# Find a free network port to use for the process manager
pm_port = find_free_port(50020, 52000)

# The command lines that should be used to start the applications
procmsg_startup_commands = ["drunc-process-manager", "<proc_mgr_choice>", str(pm_port)]
pmapp = idc.DAQControlApplication("pm", procmsg_startup_commands)

drunc_startup_commands = [
    "drunc-unified-shell",
    f"grpc://localhost:{pm_port}",
    "<config_data_file>",
    "<config_session_name>",
    "<daq_session_name>",
]
druncapp = idc.DAQControlApplication("us", drunc_startup_commands)

cmd_set_list = idc.DAQCommandSet(
    "us", dunerc_commands, idc.CommandWaitParameters(style=idc.CommandWaitStyle.ECHO)
)


# Putting everything together into a DAQSessionIngredients object
app_list = [pmapp, druncapp]
cmd_set_list = [cmd_set_list]
dsi = idc.DAQSessionIngredients(app_list, cmd_set_list)

# Declare the special variable that tells the integrationtest infrastructure what we want to run
daq_session_ingredients = {"MultiRCAppSession": dsi}


# The tests themselves

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def test_dunerc_success(run_dunerc) -> None:
    """Checks that the drunc integration command sequence completes successfully."""
    # print the name of the current test
    current_test = os.environ.get("PYTEST_CURRENT_TEST")
    match_obj = re.search(r".*\[(.+)-run_.*rc.*\d].*", current_test)
    if match_obj:
        current_test = match_obj.group(1)
    banner_line = re.sub(".", "=", current_test)
    print(banner_line)
    print(current_test)
    print(banner_line)

    # Check that dunerc completed correctly
    assert run_dunerc.completed_processes["us"].returncode == 0


def test_log_files(run_dunerc) -> None:
    """Checks that expected process-manager log files exist and are free of errors."""
    # Check that at least some of the expected log files are present
    logfile_types = ("df-01", "dfo", "mlt", "ru")
    log_names = tuple(map(str, run_dunerc.log_files))
    missing_logfiles = [
        f"{run_dunerc.daq_session_name}_{logfile_type}"
        for logfile_type in logfile_types
        if not any(
            f"{run_dunerc.daq_session_name}_{logfile_type}" in log_name
            for log_name in log_names
        )
    ]
    assert not missing_logfiles, f"No logfile found for: {', '.join(missing_logfiles)}."

    # Check that there are no warnings or errors in the log files
    assert log_file_checks.logs_are_error_free(
        [
            logname
            for logname in run_dunerc.log_files
            if "process_manager" in str(logname)
        ],
        True,
        True,
        ignored_logfile_problems,
    )


def test_connections(run_dunerc) -> None:
    lines_pm = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()
    lines_us = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()

    user_name = getpass.getuser()
    pm_connect = f"{user_name} connected from unified shell"
    us_connect = (
        f"Connecting to an existing process manager at the address localhost:{pm_port}"
    )

    assert any(pm_connect in line for line in lines_pm), (
        f"Did not find '{pm_connect}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(lines_pm)
    )

    assert any(us_connect in line for line in lines_us), (
        f"Did not find '{us_connect}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(lines_us)
    )


def test_boot_us(run_dunerc) -> None:
    """Checks that boot starts in the pms the managed processes and exposes UUIDs in ps."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()

    assert_contains_between_markers(
        lines, "pre_boot", "post_boot", "No processes running"
    )

    ps_post_boot = get_ps_table_after_echo(lines, "post_boot")
    assert ps_post_boot, (
        "Expected ps table after boot to contain processes, but it was empty."
    )
    assert_rows_have_valid_uuids(ps_post_boot)
    non_alive_processes = [
        row["friendly_name"] for row in ps_post_boot if row["status"] != "Alive"
    ]
    assert not non_alive_processes, (
        "Expected all processes after boot to be alive, but these were not: "
        + ", ".join(non_alive_processes)
    )


def test_boot_pm(run_dunerc) -> None:
    """Checks that boot starts in the pm. More lightweight, checks if root-controller boots"""
    lines = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()

    between = get_lines_between_markers(lines, "pre_boot", "post_boot")
    check_boot_sent_re = "sent boot for session pm_us via unified_shell"
    assert any(check_boot_sent_re in line for line in between), (
        f"Did not find '{check_boot_sent_re}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(between)
    )

    check_root_controller_boot = (
        "Booted 'root-controller' from session 'pm_us' with UUID"
    )
    assert any(check_root_controller_boot in line for line in between), (
        f"Did not find '{check_root_controller_boot}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(between)
    )


def test_unknown_log_command(run_dunerc) -> None:
    """Checks that querying logs for an unknown process reports the expected error."""
    test_str = (
        "Bad query for logs: The process corresponding to the query doesn't exist"
    )
    assert test_str in run_dunerc.completed_processes["us"].stdout


def test_root_controller_logs(run_dunerc) -> None:
    """
    Verifies that:
    - the stdout contains a "root-controller logs" header line and a "root-controller end" footer line
    - there are exactly 5 lines between those two lines
    - among those 5 lines, the one from "drunc.controller.core.init_controller" ends with "Controller ready"
    """
    lines = run_dunerc.completed_processes["us"].stdout.splitlines()

    # 1) Find the header/footer lines
    header_idx = require_line_containing(
        lines,
        "root-controller logs",
        error_message="Did not find the 'root-controller logs' header line in stdout.",
    )
    footer_idx = require_line_containing(
        lines,
        "root-controller end",
        error_message="Did not find the 'root-controller end' footer line in stdout.",
    )
    assert footer_idx > header_idx, "Footer appears before header in stdout."

    # 2) Check there are 5 lines between header and footer
    between = lines[header_idx + 1 : footer_idx]
    assert len(between) == 5, (
        f"Expected exactly 5 lines between header and footer, found {len(between)}.\nBetween:\n"
        + "\n".join(between)
    )

    # 3) Check one of the init_controller line ends with "Controller ready"
    # Example line:
    # [2026/03/13 08:17:47 UTC] INFO ... drunc.controller.core.init_controller ... Controller ready
    init_controller_ready_re = re.compile(
        r"drunc\.controller\.core\.init_controller.*Controller ready\s*$"
    )

    matches = [line for line in between if init_controller_ready_re.search(line)]
    assert len(matches) >= 1, (
        "Did not find an init_controller line ending with 'Controller ready' within the 5 lines.\nBetween:\n"
        + "\n".join(between)
    )


def test_wait_command_duration_from_logs(run_dunerc) -> None:
    """Checks that the wait command logs the expected duration and elapsed time."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()

    echo_idx = require_echo_marker_index(lines, "test_wait")

    running_pattern = re.compile(r"Command wait running for (\d+) seconds\.")
    ran_pattern = re.compile(r"Command wait ran for (\d+) seconds\.")
    timestamp_pattern = re.compile(r"\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) UTC\]")

    running_idx, running_match = require_pattern_match_index(
        lines,
        running_pattern,
        error_message=(
            "Did not find 'Command wait running for ... seconds.' after test_wait marker."
        ),
        start_idx=echo_idx + 1,
    )

    ran_idx, ran_match = require_pattern_match_index(
        lines,
        ran_pattern,
        error_message=(
            "Did not find 'Command wait ran for ... seconds.' after wait start log."
        ),
        start_idx=running_idx + 1,
    )

    expected_seconds = 10
    assert int(running_match.group(1)) == expected_seconds, (
        f"Expected wait start log to report {expected_seconds} seconds, got {running_match.group(1)}."
    )
    assert int(ran_match.group(1)) == expected_seconds, (
        f"Expected wait end log to report {expected_seconds} seconds, got {ran_match.group(1)}."
    )

    start_ts_match = require_pattern_match(
        lines[running_idx],
        timestamp_pattern,
        error_message="Could not parse timestamp in wait start log line.",
    )
    end_ts_match = require_pattern_match(
        lines[ran_idx],
        timestamp_pattern,
        error_message="Could not parse timestamp in wait end log line.",
    )

    ts_strp_pattern = "%Y/%m/%d %H:%M:%S"
    start_ts = datetime.strptime(start_ts_match.group(1), ts_strp_pattern)
    end_ts = datetime.strptime(end_ts_match.group(1), ts_strp_pattern)
    elapsed_seconds = (end_ts - start_ts).total_seconds()

    tolerance_seconds = 1
    assert abs(elapsed_seconds - expected_seconds) <= tolerance_seconds, (
        f"Expected wait log timestamps to differ by {expected_seconds}±{tolerance_seconds} seconds, "
        f"got {elapsed_seconds} seconds."
    )


def test_restart_mlt_logs_pm(run_dunerc) -> None:
    """Checks that restarting mlt produces the expected restart, exit, and boot logs."""
    lines = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()
    restart_text = get_text_between_echo_markers(
        lines, "pre_restart_mlt", "post_restart_mlt"
    )

    require_pattern_match(
        restart_text,
        re.compile(
            r"Remote process .*?terminated gracefully following SIGQUIT signal\.",
            re.DOTALL,
        ),
        error_message="Did not find the graceful termination log line for mlt after restart request.",
    )

    require_pattern_match(
        restart_text,
        re.compile(
            r"Process 'mlt' \(.*?\) was terminated by the process manager through the remote pid\. Reported exit code: 0\.",
            re.DOTALL,
        ),
        error_message="Did not find the mlt exit-code log line after graceful termination.",
    )

    # Note difference in the reboot message between the PM and the unified shell.
    assert_match_contains_uuid(
        restart_text,
        pattern=re.compile(
            r"Booted 'mlt' from session \S+ with UUID\s+([^\s\n]+)(?:\s+on host\s+\S+)?",
            re.DOTALL,
        ),
        error_message="Did not find the mlt boot log line in pm after the restart exit log.",
    )


def test_restart_mlt_logs_us(run_dunerc) -> None:
    """Checks that restarting mlt produces the expected restart, exit, and boot logs."""

    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    restart_text = get_text_between_echo_markers(
        lines, "pre_restart_mlt", "post_restart_mlt"
    )

    # Note difference in the reboot message between the PM and the unified shell.
    assert_match_contains_uuid(
        restart_text,
        pattern=re.compile(
            r"Restarted \['mlt'\] from session \S+ with UUID\s+([^\s\n]+)(?:\s+on host\s+\S+)?",
            re.DOTALL,
        ),
        error_message="Did not find the mlt boot log line in us after the restart exit log.",
    )

    ps_after_restart = get_ps_table_after_echo(lines, "post_restart_mlt")
    mlt_status = get_column_for_friendly_name(ps_after_restart, "mlt", "status")
    assert mlt_status == "Alive", (
        f"Expected mlt to be alive after restart, but its status was '{mlt_status}'."
    )


def test_kill_removes_mlt_from_ps_table(run_dunerc) -> None:
    """Checks that killing mlt removes it from the subsequent ps table."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()

    ps_before_kill = get_ps_table_after_echo(lines, "test_kill_mlt")
    ps_after_kill = get_ps_table_after_echo(lines, "test_kill_mlt_post")

    assert_process_presence(ps_before_kill, "mlt", context="before kill")
    assert_process_presence(
        ps_after_kill, "mlt", context="after kill", expected_present=False
    )


def test_mlt_recovers_after_kill(run_dunerc) -> None:
    """Checks that mlt is present again after the recovery restart sequence."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    ps_after_recovery = get_ps_table_after_echo(lines, "test_recovery_post")
    assert_process_presence(ps_after_recovery, "mlt", context="after recovery")


def test_flush(run_dunerc) -> None:
    """Checks that flush work by crashing mlt, seeing that the process exists,
    and then flushing to show its gone"""

    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    ps_initial = get_ps_table_after_echo(lines, "test_flush")
    assert_process_presence(ps_initial, "mlt", context="before crash")

    ps_after_crash = get_ps_table_after_echo(lines, "after_crash")
    mlt_alive = get_column_for_friendly_name(ps_after_crash, "mlt", "status")
    assert mlt_alive == "Dead", "The mlt should have crashed"

    ps_after_flash = get_ps_table_after_echo(lines, "after_flush")
    assert_process_presence(
        ps_after_flash, "mlt", context="after crash", expected_present=False
    )


def test_terminate(run_dunerc) -> None:
    """Test terminate by checking both pm and us shells"""
    lines_us = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    lines_pm = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()

    pre_boot_idx_pm = require_line_containing(
        lines_pm,
        "test_terminate",
        error_message="Did not find the 'test_terminate' header line in stdout.",
    )
    post_boot_idx_pm = require_line_containing(
        lines_pm,
        "test_terminate_done",
        error_message="Did not find the 'test_terminate_done' footer line in stdout.",
    )

    pre_boot_idx_us = require_line_containing(
        lines_us,
        "test_terminate",
        error_message="Did not find the 'test_terminate' header line in stdout.",
    )

    between_pm = lines_pm[pre_boot_idx_pm + 1 : post_boot_idx_pm]
    shutdown_re = "--- Shutdown stage: Role 'root-controller' complete ---"
    assert any(shutdown_re in line for line in between_pm), (
        f"Did not find '{shutdown_re}' between test_terminate and test_terminate_done.\nBetween:\n"
        + "\n".join(between_pm)
    )

    # TODO: This bit here is grabbing functions from the integ test utils. Maybe it can be better optimised?
    table_start_idx = find_line_index(
        lines_us,
        lambda line: "Terminated process" in line,
        start_idx=pre_boot_idx_us + 1,
    )

    assert table_start_idx is not None, "cannot fine terminated process table"

    terminated_table = _parse_table_from_index(lines_pm, table_start_idx, "ps")
    for row in terminated_table:
        assert UUID_RE.match(row["uuid"]), (
            f"Expected a valid UUID for process '{row['friendly_name']}', got '{row['uuid']}'"
        )


# ── FSM command/sequence tests (mirrors controller_test.py) ────────────────────


def _check_fsm_command(
    lines: list[str],
    boot_status_table: list[dict[str, str]],
    params: FsmCommandParams,
) -> None:
    """Shared assertion logic for a drunc FSM command.

    Checks:
    - Execution report names match boot table, all rows successful.
    - Post-command status table has expected state/substates.
    - Run number if specified.
    """
    exec_report = get_execution_report_after_echo(lines, params.marker)
    assert exec_report, f"No execution report found after '{params.marker}' marker."

    boot_names = {row["name"] for row in boot_status_table}
    report_names = {row["name"] for row in exec_report}
    assert report_names == boot_names, (
        f"Execution report names do not match boot status table names.\n"
        f"  Only in report:     {report_names - boot_names}\n"
        f"  Only in boot table: {boot_names - report_names}"
    )
    check_execution_report_success(exec_report)

    status_table = get_status_table_after_echo(lines, params.done_marker)
    assert status_table, f"No status table found after '{params.done_marker}' marker."
    check_status_table_states(status_table, expected_state=params.expected_state)
    check_status_table_substates(
        status_table,
        controller_substate=params.expected_state,
        non_controller_substate=params.non_controller_substate,
    )

    if params.run_number is not None:
        run_info = get_run_info_after_echo(lines, params.done_marker)
        assert run_info, f"No Run Info table found after '{params.done_marker}' marker."
        assert run_info["Run number"] == str(params.run_number), (
            f"Expected run number '{params.run_number}', got '{run_info['Run number']}'."
        )


@pytest.fixture(scope="module")
def boot_status_table(run_dunerc):
    """Parse and cache the status table produced right before the FSM command
    sequence starts, once the process-manager tests have restored a clean
    process set (post restart/kill/recovery)."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    return get_status_table_after_echo(lines, "pre_fsm_status")


@pytest.mark.parametrize("params", _FSM_COMMANDS, ids=lambda p: p.marker)
def test_fsm_command(run_dunerc, boot_status_table, params: FsmCommandParams) -> None:
    """Checks that each FSM command executes successfully and transitions all processes to the expected state."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    _check_fsm_command(lines, boot_status_table, params)


@pytest.mark.parametrize("params", _FSM_SEQUENCES.values(), ids=lambda p: p.marker)
def test_fsm_transitions(
    run_dunerc, boot_status_table, params: FsmCommandParams
) -> None:
    """Checks that each FSM transition executes successfully and reaches its expected state."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    _check_fsm_command(lines, boot_status_table, params)


def test_shutdown_status(run_dunerc) -> None:
    """Checks that status reports the session is no longer booted after shutdown."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    shutdown_index = next(
        index for index, line in enumerate(lines) if _SHUTDOWN_MARKER in line
    )
    shutdown_output = "\n".join(lines[shutdown_index:])
    assert _SHUTDOWN_STATUS_ERROR in shutdown_output
