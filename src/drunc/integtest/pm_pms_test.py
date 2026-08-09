# 05-Aug-2026, KAB: the goal of this test is to validate and demonstrate the use of multiple
# user-specified applications running in the DAQ session that is part of this test.
#
# This integtest was created by copying the small_footprint_quick_test from the daqsystemtest
# repo and converting the assignment of the run control commands to make use of the new
# "daq_session_ingredients" special integtest variable.
#

import functools
import os
import re
from datetime import datetime

import integrationtest.log_file_checks as log_file_checks
import integrationtest.resource_validation as resource_validation
import integrationtest.utility_functions as utility_functions
from daqconf.utils import find_free_port
from integ_test_utils import (
    _PS_COLUMNS,
    _parse_table_from_index,
    assert_contains_between_markers,
    assert_match_contains_uuid,
    assert_process_presence,
    assert_rows_have_valid_uuids,
    find_line_index,
    get_column_for_friendly_name,
    get_lines_between_markers,
    get_ps_table_after_echo,
    get_text_between_echo_markers,
    require_echo_marker_index,
    require_line_containing,
    require_pattern_match,
    require_pattern_match_index,
    strip_ansi,
)
from integrationtest.data_classes import *
from integrationtest.get_pytest_tmpdir import get_pytest_tmpdir

print = functools.partial(print, flush=True)  # always flush print() output

pytest_plugins = "integrationtest.integrationtest_drunc"

# Values that help determine the running conditions
number_of_data_producers = 2
data_rate_slowdown_factor = 1  # 10 for ProtoWIB/DuneWIB
run_duration = 10  # seconds
readout_window_time_before = 1000
readout_window_time_after = 1001

ignored_logfile_problems = {
    "connectionservice": [
        "Searching for connections matching uid_regex<errored_frames_q> and data_type Unknown"
    ],
    "SSH_SHELL_process_manager": [
        "was terminated unexpectedly through the remote pid by a SIGKILL",
    ],
    "-controller": [
        "Worker with pid \\d+ was terminated due to signal 1",
        "Connection '.*' not found on the application registry",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

# Determine if this computer has enough resources for these tests
resource_validator = resource_validation.ResourceValidator()
resource_validator.cpu_count_needs(
    4, 8
)  # 2 for each data source plus 2 more for everything else
resource_validator.free_memory_needs(
    4, 6
)  # 33% more than what we observe being used ('free -h')
actual_output_path = get_pytest_tmpdir()
resource_validator.free_disk_space_needs(
    actual_output_path, 1
)  # more than what we observe

# The arguments to pass to the config generator, excluding the json
# output directory (the test framework handles that)

conf_dict = integtest_params_for_generated_dunedaq_config()
conf_dict.object_databases = ["config/daqsystemtest/integrationtest-objects.data.xml"]
conf_dict.dro_map_config.n_streams = number_of_data_producers
conf_dict.op_env = "integtest"
conf_dict.config_session_name = "pm_pms"
conf_dict.tpg_enabled = False
utility_functions.enable_fake_hsi_trigger(conf_dict, trigger_rate=1.0)

conf_dict.config_substitutions.append(
    attribute_substitution(obj_class="LatencyBuffer", updates={"size": 50000})
)

confgen_arguments = {"SmallFootprint": conf_dict}

daq_session_name = "pms-test"
daq_session_name_1 = "pms-test-1"

# The commands to run in dunerc and the process manager shell
dunerc_commands = f"""

    echo pre_boot
    log pre_boot
    ps -w 180
    boot config/daqsystemtest/example-configs.data.xml local-1x1-config {daq_session_name}
    wait 5
    echo post_boot
    log post_boot
    ps -w 180


    echo test_logs
    logs --name unknown
    logs --name root-controller --how-far 5
    logs --name mlt --how-far 5
    echo test_logs_done

    echo test_wait
    wait 10
    echo test_wait_done

    echo pre_restart_mlt
    log pre_restart_mlt
    restart -n mlt
    restart -n root-controller
    wait 5
    echo post_restart_mlt
    log post_restart_mlt


    echo test_kill_mlt
    ps -w 180
    kill -n mlt
    wait 2
    echo test_kill_mlt_post
    ps -w 180
    echo test_kill_mlt_done


    echo test_recovery
    restart -n mlt
    restart -n trg-controller
    wait 5
    echo test_recovery_post
    ps -w 180
    echo test_recovery_done


    echo test_flush
    ps -w 180
    kill -n mlt --crash 
    wait 5
    echo after_crash
    ps -w 180
    flush
    echo after_flush
    ps -w 180
    echo test_flush_done


    echo pre_boot_2
    log pre_boot_2
    ps -w 180
    boot config/daqsystemtest/example-configs.data.xml local-1x1-config {daq_session_name_1}
    wait 5
    echo post_boot_2
    log post_boot_2
    ps -w 180


    echo test_terminate
    log test_terminate
    terminate
    echo test_terminate_done
    log test_terminate_done

    """.split()

# Find a free network port to use for the process manager
pm_port = find_free_port(50020, 52000)

# The command lines that should be used to start the applications
procmsg_startup_commands = ["drunc-process-manager", "<proc_mgr_choice>", str(pm_port)]
pmapp = DAQSessionApp("pm", procmsg_startup_commands)

pmshell_startup_commands = [
    "drunc-process-manager-shell",
    f"grpc://localhost:{pm_port}",
]
pmshellapp = DAQSessionApp("pmshell", pmshell_startup_commands)

# Packaging up the commands into DAQCommandSets
cmd_set = DAQCommandSet(
    "pmshell", dunerc_commands, CommandWaitParameters(style=CommandWaitStyle.ECHO)
)

# Putting everything together into a DAQSessionIngredients object
app_list = [pmapp, pmshellapp]
cmd_set_list = [cmd_set]
dsi = DAQSessionIngredients(app_list, cmd_set_list)

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
    assert run_dunerc.completed_processes["pmshell"].returncode == 0


def test_log_files(run_dunerc) -> None:
    """Checks that expected process-manager log files exist and are free of errors."""
    # Check that at least some of the expected log files are present
    assert any(
        f"{daq_session_name}_df-01" in str(logname) for logname in run_dunerc.log_files
    )
    assert any(
        f"{daq_session_name}_dfo" in str(logname) for logname in run_dunerc.log_files
    )
    assert any(
        f"{daq_session_name}_mlt" in str(logname) for logname in run_dunerc.log_files
    )
    assert any(
        f"{daq_session_name}_ru" in str(logname) for logname in run_dunerc.log_files
    )

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
    lines_pms = strip_ansi(
        run_dunerc.completed_processes["pmshell"].stdout
    ).splitlines()

    pm_connect = "connected from process_manager_shell"
    pms_connect = "connected to the process manager through a"

    assert any(pm_connect in line for line in lines_pm), (
        f"Did not find '{pm_connect}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(lines_pm)
    )

    assert any(pms_connect in line for line in lines_pms), (
        f"Did not find '{pms_connect}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(lines_pms)
    )


def test_boot_pms(run_dunerc) -> None:
    """Checks that boot starts in the pms the managed processes and exposes UUIDs in ps."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()

    assert_contains_between_markers(
        lines, "pre_boot", "post_boot", "No processes running"
    )
    ps_post_boot = get_ps_table_after_echo(lines, "post_boot")
    assert ps_post_boot, (
        "Expected ps table after boot to contain processes, but it was empty."
    )
    assert_rows_have_valid_uuids(ps_post_boot)


def test_boot_pm(run_dunerc) -> None:
    """Checks that boot starts in the pm. More lightweight, checks if root-controller boots"""
    lines = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()

    assert_contains_between_markers(
        lines, "pre_boot", "post_boot", "sent boot with arguments"
    )
    check_root_controller_boot = (
        f"Booted 'root-controller' from session '{daq_session_name}' with UUID"
    )
    assert_contains_between_markers(
        lines, "pre_boot", "post_boot", check_root_controller_boot
    )


def test_boot_pms_2(run_dunerc) -> None:
    """Checks that boot starts in the pms the managed processes and exposes UUIDs in ps."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()

    get_lines_between_markers(lines, "pre_boot_2", "post_boot_2")
    ps_post_boot = get_ps_table_after_echo(lines, "post_boot_2")
    assert ps_post_boot, (
        "Expected ps table after boot to contain processes, but it was empty."
    )
    assert_rows_have_valid_uuids(ps_post_boot)


def test_boot_pm_2(run_dunerc) -> None:
    """Checks that boot starts in the pm. More lightweight, checks if root-controller boots"""
    lines = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()

    assert_contains_between_markers(
        lines, "pre_boot_2", "post_boot_2", "sent boot with arguments"
    )
    check_root_controller_boot = (
        f"Booted 'root-controller' from session '{daq_session_name_1}' with UUID"
    )
    assert_contains_between_markers(
        lines, "pre_boot_2", "post_boot_2", check_root_controller_boot
    )


def test_unknown_log_command(run_dunerc) -> None:
    """Checks that querying logs for an unknown process reports the expected error."""
    test_str = (
        "Bad query for logs: The process corresponding to the query doesn't exist"
    )
    assert test_str in run_dunerc.completed_processes["pmshell"].stdout


def test_root_controller_logs(run_dunerc) -> None:
    """
    Verifies that:
    - the stdout contains a "root-controller logs" header line and a "root-controller end" footer line
    - there are exactly 5 lines between those two lines
    - among those 5 lines, the one from "drunc.controller.core.init_controller" ends with "Controller ready"
    """
    lines = run_dunerc.completed_processes["pmshell"].stdout.splitlines()

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
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()

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

    # Note difference in the reboot message between the PM and PMS.
    assert_match_contains_uuid(
        restart_text,
        pattern=re.compile(
            r"Booted 'mlt' from session \S+ with UUID\s+([^\s\n]+)(?:\s+on host\s+\S+)?",
            re.DOTALL,
        ),
        error_message="Did not find the mlt boot log line in pm after the restart exit log.",
    )


def test_restart_mlt_logs_pms(run_dunerc) -> None:
    """Checks that restarting mlt produces the expected restart, exit, and boot logs."""

    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()
    restart_text = get_text_between_echo_markers(
        lines, "pre_restart_mlt", "post_restart_mlt"
    )

    # Note difference in the reboot message between the PM and PMS.
    assert_match_contains_uuid(
        restart_text,
        pattern=re.compile(
            r"Restarted \['mlt'\] from session \S+ with UUID\s+([^\s\n]+)(?:\s+on host\s+\S+)?",
            re.DOTALL,
        ),
        error_message="Did not find the mlt boot log line in pms after the restart exit log.",
    )


def test_kill_removes_mlt_from_ps_table(run_dunerc) -> None:
    """Checks that killing mlt removes it from the subsequent ps table."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()

    ps_before_kill = get_ps_table_after_echo(lines, "test_kill_mlt")
    ps_after_kill = get_ps_table_after_echo(lines, "test_kill_mlt_post")

    assert_process_presence(ps_before_kill, "mlt", context="before kill")
    assert_process_presence(
        ps_after_kill, "mlt", context="after kill", expected_present=False
    )


def test_mlt_recovers_after_kill(run_dunerc) -> None:
    """Checks that mlt is present again after the recovery restart sequence."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()
    ps_after_recovery = get_ps_table_after_echo(lines, "test_recovery_post")
    assert_process_presence(ps_after_recovery, "mlt", context="after recovery")


def test_terminate(run_dunerc) -> None:
    """Test terminate by checking both pm and pms shells"""
    lines_pms = strip_ansi(
        run_dunerc.completed_processes["pmshell"].stdout
    ).splitlines()
    lines_pm = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()

    pre_boot_idx_pm = require_line_containing(
        lines_pm,
        "test_terminate",
        error_message="Did not find the 'pre_boot' header line in stdout.",
    )
    post_boot_idx_pm = require_line_containing(
        lines_pm,
        "test_terminate_done",
        error_message="Did not find the 'post_boot' footer line in stdout.",
    )

    pre_boot_idx_pms = require_line_containing(
        lines_pms,
        "test_terminate",
        error_message="Did not find the 'pre_boot' header line in stdout.",
    )

    between_pm = lines_pm[pre_boot_idx_pm + 1 : post_boot_idx_pm]
    shutdown_re = "--- Shutdown stage: Role 'root-controller' complete ---"
    assert any(shutdown_re in line for line in between_pm), (
        f"Did not find '{shutdown_re}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(between_pm)
    )

    # TODO: This bit here is grabbing functions from the integ test utils. Maybe it can be better optimised?
    table_start_idx = find_line_index(
        lines_pms,
        lambda line: "Terminated process" in line,
        start_idx=pre_boot_idx_pms + 1,
    )

    assert table_start_idx is not None, "cannot fine terminated process table"

    terminated_table = _parse_table_from_index(lines_pm, table_start_idx, _PS_COLUMNS)
    for row in terminated_table:
        assert UUID_RE.match(row["uuid"]), (
            f"Expected a valid UUID for process '{row['friendly_name']}', got '{row['uuid']}'"
        )


def test_flush(run_dunerc) -> None:
    """Checks that flush work by crashing mlt, seeing that the process exists,
    and then flushing to show its gone"""

    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()
    ps_initial = get_ps_table_after_echo(lines, "test_flush")
    assert_process_presence(ps_initial, "mlt", context="before crash")

    ps_after_crash = get_ps_table_after_echo(lines, "after_crash")
    mlt_alive = get_column_for_friendly_name(ps_after_crash, "mlt", "alive")
    assert mlt_alive == "False", "The mlt should have crashed"

    ps_after_flash = get_ps_table_after_echo(lines, "after_flush")
    assert_process_presence(
        ps_after_flash, "mlt", context="after crash", expected_present=False
    )
