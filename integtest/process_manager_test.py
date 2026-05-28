import getpass
import os
import re
from datetime import datetime

import integrationtest.data_classes as data_classes
import integrationtest.log_file_checks as log_file_checks
from integ_test_utils import (
    assert_process_presence,
    get_column_for_friendly_name,
    get_ps_table_after_echo,
    require_echo_marker_index,
    require_line_containing,
    require_pattern_match,
    require_pattern_match_index,
    strip_ansi,
)

pytest_plugins = "integrationtest.integrationtest_drunc"

# Values that help determine the running conditions
number_of_data_producers = 2
data_rate_slowdown_factor = 1  # 10 for ProtoWIB/DuneWIB
run_duration = 10  # seconds
readout_window_time_before = 1000
readout_window_time_after = 1001

check_for_logfile_errors = True

ignored_logfile_problems = {
    "-controller": [
        "Worker with pid \\d+ was terminated due to signal",
        "Connection '.*' not found on the application registry",
    ],
    "SSH_SHELL_process_manager": [
        "was terminated unexpectedly through the remote pid by a SIGKILL",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

conf_dict = data_classes.integtest_params_for_generated_dunedaq_config()
conf_dict.object_databases = ["config/daqsystemtest/integrationtest-objects.data.xml"]
conf_dict.dro_map_config.n_streams = number_of_data_producers
conf_dict.op_env = "integtest"
conf_dict.session = "minimal"
conf_dict.tpg_enabled = False

# For testing, allow drunc to manage ConnectivityService (default is False, integrationtest manages Connectivity Service)
conf_dict.drunc_connsvc = True
# For testing, specify connectivity service port (default is 0, a random port is chosen for the Connectivity Service)
# conf_dict.connsvc_port = 12345

conf_dict.config_substitutions.append(
    data_classes.attribute_substitution(
        obj_id="random-tc-generator",
        obj_class="RandomTCMakerConf",
        updates={"trigger_rate_hz": 1},
    )
)
conf_dict.config_substitutions.append(
    data_classes.attribute_substitution(
        obj_class="TCReadoutMap",
        obj_id="def-random-readout",
        updates={
            "time_before": readout_window_time_before,
            "time_after": readout_window_time_after,
        },
    )
)


confgen_arguments = {"MinimalSystem": conf_dict}
# The commands to run in dunerc
# The commands mostly come from the msqt, with a few minor changes
# The entire format is a standard that is  basically copied over from the
# typical msqt tests, so they bear no direct effect on the scope of this test.
dunerc_command_list = f"""

echo pre_boot
ps -u {getpass.getuser()} -w 180
boot
echo post_boot
ps -u {getpass.getuser()} -w 180


echo test_logs
logs --name unknown
logs --name root-controller --how-far 5
logs --name mlt --how-far 5
echo test_logs_done

echo test_wait
wait 10
echo test_wait_done

echo pre_restart_mlt
restart -n mlt
restart -n root-controller
wait 5
echo post_restart_mlt


echo test_kill_mlt
ps -u {getpass.getuser()} -w 180
kill -n mlt
wait 2
echo test_kill_mlt_post
ps -u {getpass.getuser()} -w 180
echo test_kill_mlt_done


echo test_recovery
restart -n mlt
restart -n trg-controller
wait 5
echo test_recovery_post
ps -u {getpass.getuser()} -w 180
echo test_recovery_done


echo test_flush
ps -u {getpass.getuser()} -w 180
kill -n mlt --crash 
wait 5
echo after_crash
ps -u {getpass.getuser()} -w 180
flush
echo after_flush
ps -u {getpass.getuser()} -w 180
echo test_flush_done


terminate

""".split()


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
    assert run_dunerc.completed_process.returncode == 0


def test_log_files(run_dunerc) -> None:
    """Checks that expected process-manager log files exist and are free of errors."""
    # Check that at least some of the expected log files are present
    assert any(
        f"{run_dunerc.daq_session_name}_df-01" in str(logname)
        for logname in run_dunerc.log_files
    )
    assert any(
        f"{run_dunerc.daq_session_name}_dfo" in str(logname)
        for logname in run_dunerc.log_files
    )
    assert any(
        f"{run_dunerc.daq_session_name}_mlt" in str(logname)
        for logname in run_dunerc.log_files
    )
    assert any(
        f"{run_dunerc.daq_session_name}_ru" in str(logname)
        for logname in run_dunerc.log_files
    )

    if check_for_logfile_errors:
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


def test_boot(run_dunerc) -> None:
    """Checks that boot starts the managed processes and exposes UUIDs in ps."""
    stdout = run_dunerc.completed_process.stdout

    ps_pre_boot = get_ps_table_after_echo(stdout, "pre_boot")
    ps_post_boot = get_ps_table_after_echo(stdout, "post_boot")

    assert not ps_pre_boot, (
        f"Expected ps table before boot to be empty, but found {len(ps_pre_boot)} row(s): "
        + ", ".join(row["friendly_name"] for row in ps_pre_boot)
    )

    assert ps_post_boot, (
        "Expected ps table after boot to contain processes, but it was empty."
    )
    for row in ps_post_boot:
        assert UUID_RE.match(row["uuid"]), (
            f"Expected a valid UUID for process '{row['friendly_name']}', got '{row['uuid']}'"
        )


def test_unknown_log_command(run_dunerc) -> None:
    """Checks that querying logs for an unknown process reports the expected error."""
    test_str = (
        "Bad query for logs: The process corresponding to the query doesn't exist"
    )
    assert test_str in run_dunerc.completed_process.stdout


def test_root_controller_logs(run_dunerc) -> None:
    """
    Verifies that:
    - the stdout contains a "root-controller logs" header line and a "root-controller end" footer line
    - there are exactly 5 lines between those two lines
    - among those 5 lines, the one from "drunc.controller.core.init_controller" ends with "Controller ready"
    """
    lines = run_dunerc.completed_process.stdout.splitlines()

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
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()

    echo_idx = require_echo_marker_index(lines, "test_wait")

    running_pattern = re.compile(r"Command wait running for (\d+) seconds\.")
    ran_pattern = re.compile(r"Command wait ran for (\d+) seconds\.")
    timestamp_pattern = re.compile(r"^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) UTC\]")

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


def test_restart_mlt_logs(run_dunerc) -> None:
    """Checks that restarting mlt produces the expected restart, exit, and boot logs."""
    stdout = run_dunerc.completed_process.stdout
    lines = strip_ansi(stdout).splitlines()

    echo_idx = require_echo_marker_index(lines, "pre_restart_mlt")

    post_restart_idx = require_echo_marker_index(
        lines, "post_restart_mlt", start_idx=echo_idx + 1
    )

    restart_lines = lines[echo_idx + 1 : post_restart_idx]
    restart_text = "\n".join(restart_lines)

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
        re.compile(r"Process 'mlt'.*?process exited\s+with exit code 0", re.DOTALL),
        error_message="Did not find the mlt exit-code log line after graceful termination.",
    )

    booted_match = require_pattern_match(
        restart_text,
        re.compile(r"Booted 'mlt'.*?with UUID\s+([^\s\n]+)", re.DOTALL),
        error_message="Did not find the mlt boot log line after the restart exit log.",
    )

    booted_uuid = booted_match.group(1)
    assert UUID_RE.match(booted_uuid), (
        f"Expected the mlt boot log to contain a UUID, got: {booted_uuid}"
    )


def test_kill_removes_mlt_from_ps_table(run_dunerc) -> None:
    """Checks that killing mlt removes it from the subsequent ps table."""
    stdout = run_dunerc.completed_process.stdout

    ps_before_kill = get_ps_table_after_echo(stdout, "test_kill_mlt")
    ps_after_kill = get_ps_table_after_echo(stdout, "test_kill_mlt_post")

    assert_process_presence(ps_before_kill, "mlt", context="before kill")
    assert_process_presence(
        ps_after_kill, "mlt", context="after kill", expected_present=False
    )


def test_mlt_recovers_after_kill(run_dunerc) -> None:
    """Checks that mlt is present again after the recovery restart sequence."""
    stdout = run_dunerc.completed_process.stdout
    ps_after_recovery = get_ps_table_after_echo(stdout, "test_recovery_post")
    assert_process_presence(ps_after_recovery, "mlt", context="after recovery")


def test_flush(run_dunerc) -> None:
    """Checks that flush work by crashing mlt, seeing that the process exists,
    and then flushing to show its gone"""

    stdout = run_dunerc.completed_process.stdout
    ps_initial = get_ps_table_after_echo(stdout, "test_flush")
    assert_process_presence(ps_initial, "mlt", context="before crash")

    ps_after_crash = get_ps_table_after_echo(stdout, "after_crash")
    mlt_alive = get_column_for_friendly_name(ps_after_crash, "mlt", "alive")
    assert mlt_alive == "False", "The mlt should have crashed"

    ps_after_flash = get_ps_table_after_echo(stdout, "after_flush")
    assert_process_presence(
        ps_after_flash, "mlt", context="after crash", expected_present=False
    )
