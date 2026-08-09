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

import integrationtest.log_file_checks as log_file_checks
import integrationtest.resource_validation as resource_validation
import integrationtest.utility_functions as utility_functions
from daqconf.utils import find_free_port
from integ_test_utils import (
    _PS_COLUMNS,
    _parse_table_from_index,
    assert_contains_between_markers,
    assert_rows_have_valid_uuids,
    find_line_index,
    get_lines_between_markers,
    get_ps_table_after_echo,
    require_line_containing,
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
conf_dict.config_session_name = "pm_us"
conf_dict.tpg_enabled = False
utility_functions.enable_fake_hsi_trigger(conf_dict, trigger_rate=1.0)

conf_dict.config_substitutions.append(
    attribute_substitution(obj_class="LatencyBuffer", updates={"size": 50000})
)

confgen_arguments = {"SmallFootprint": conf_dict}

# The commands to run in dunerc and the process manager shell
dunerc_commands = """

    echo pre_boot
    echo-on-server pre_boot
    ps -w 180
    boot
    wait 5
    echo post_boot
    echo-on-server post_boot
    ps -w 180

    echo test_terminate
    echo-on-server test_terminate
    terminate
    echo test_terminate_done
    echo-on-server test_terminate_done

    """.split()

# Find a free network port to use for the process manager
pm_port = find_free_port(50020, 52000)

# The command lines that should be used to start the applications
procmsg_startup_commands = ["drunc-process-manager", "<proc_mgr_choice>", str(pm_port)]
pmapp = DAQSessionApp("pm", procmsg_startup_commands)

drunc_startup_commands = [
    "drunc-unified-shell",
    f"grpc://localhost:{pm_port}",
    "<config_data_file>",
    "<config_session_name>",
    "<daq_session_name>",
]
druncapp = DAQSessionApp("us", drunc_startup_commands)

cmd_set_list = DAQCommandSet(
    "us", dunerc_commands, CommandWaitParameters(style=CommandWaitStyle.ECHO)
)


# Putting everything together into a DAQSessionIngredients object
app_list = [pmapp, druncapp]
cmd_set_list = [cmd_set_list]
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
    assert run_dunerc.completed_processes["us"].returncode == 0


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

    pm_connect = "connected from unified shell"
    us_connect = "Connecting to an existing process manager"

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


def test_terminate(run_dunerc) -> None:
    """Test terminate by checking both pm and pms shells"""
    lines_pms = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
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
