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
    assert_rows_have_valid_uuids,
    find_line_index,
    get_ps_table_after_echo,
    require_echo_marker_index,
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
conf_dict.config_session_name = "pmaas-us"
conf_dict.tpg_enabled = False
utility_functions.enable_fake_hsi_trigger(conf_dict, trigger_rate=1.0)

conf_dict.config_substitutions.append(
    attribute_substitution(obj_class="LatencyBuffer", updates={"size": 50000})
)

confgen_arguments = {"SmallFootprint": conf_dict}

daq_session_name_1 = "pmaas-pms-1"
daq_session_name_2 = "pmaas-pms-2"

# Commands in requested order:
# 1) pms: boot session 1, boot session 2
# 2) us: boot
# 3) pms: ps, ps -s session1, logs (ambiguous), logs scoped
# 4) us: ps, logs scoped, terminate
# 5) pms: ps, terminate, ps
pmshell_commands_stage_1 = f"""
    echo pms_boot_1
    boot config/daqsystemtest/example-configs.data.xml local-1x1-config {daq_session_name_1}
    wait 5
    echo pms_boot_2
    boot config/daqsystemtest/example-configs.data.xml local-1x1-config {daq_session_name_2}
    wait 15
    """.split()

us_commands_stage_1 = """
    echo us_boot
    boot
    wait 15
    """.split()

pmshell_commands_stage_2 = f"""
    echo pms_ps_all_sessions
    ps -w 180

    echo pms_ps_session_1_only
    ps -s {daq_session_name_1} -w 180

    echo pms_logs_ambiguous
    logs -n root-controller

    echo pms_logs_scoped
    logs -n root-controller -s {daq_session_name_1} --how-far 5
    """.split()

us_commands_stage_2 = """

    echo us_ps_only
    ps -w 180

    echo us_logs_scoped
    logs -n root-controller --how-far 5

    echo us_terminate
    terminate
    echo us_terminate_done
    """.split()

pmshell_commands_stage_3 = """
    echo pms_ps_after_us_terminate
    ps -w 180

    echo pms_terminate_all
    terminate

    echo pms_ps_final
    ps -w 180
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

us_startup_commands = [
    "drunc-unified-shell",
    f"grpc://localhost:{pm_port}",
    "<config_data_file>",
    "<config_session_name>",
    "<daq_session_name>",
]
usapp = DAQSessionApp("us", us_startup_commands)

# Packaging up the commands into DAQCommandSets
cmd_set_1 = DAQCommandSet(
    "pmshell",
    pmshell_commands_stage_1,
    CommandWaitParameters(style=CommandWaitStyle.ECHO),
)
cmd_set_2 = DAQCommandSet(
    "us",
    us_commands_stage_1,
    CommandWaitParameters(style=CommandWaitStyle.ECHO),
)
cmd_set_3 = DAQCommandSet(
    "pmshell",
    pmshell_commands_stage_2,
    CommandWaitParameters(style=CommandWaitStyle.ECHO),
)
cmd_set_4 = DAQCommandSet(
    "us",
    us_commands_stage_2,
    CommandWaitParameters(style=CommandWaitStyle.ECHO),
)
cmd_set_5 = DAQCommandSet(
    "pmshell",
    pmshell_commands_stage_3,
    CommandWaitParameters(style=CommandWaitStyle.ECHO),
)

# Putting everything together into a DAQSessionIngredients object
app_list = [pmapp, pmshellapp, usapp]
cmd_set_list = [cmd_set_1, cmd_set_2, cmd_set_3, cmd_set_4, cmd_set_5]
dsi = DAQSessionIngredients(app_list, cmd_set_list)

# Declare the special variable that tells the integrationtest infrastructure what we want to run
daq_session_ingredients = {"MultiRCAppSession": dsi}


# The tests themselves

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def test_dunerc_success(run_dunerc) -> None:
    """Checks that the drunc integration command sequence completes successfully."""
    current_test = os.environ.get("PYTEST_CURRENT_TEST")
    match_obj = re.search(r".*\[(.+)-run_.*rc.*\d].*", current_test)
    if match_obj:
        current_test = match_obj.group(1)
    banner_line = re.sub(".", "=", current_test)
    print(banner_line)
    print(current_test)
    print(banner_line)

    assert run_dunerc.completed_processes["pmshell"].returncode == 0
    assert run_dunerc.completed_processes["us"].returncode == 0


def test_log_files(run_dunerc) -> None:
    """Checks that expected process-manager log files exist and are free of errors."""
    assert any(
        f"{daq_session_name_1}_df-01" in str(logname)
        for logname in run_dunerc.log_files
    )
    assert any(
        f"{daq_session_name_2}_df-01" in str(logname)
        for logname in run_dunerc.log_files
    )
    assert any(
        f"{conf_dict.config_session_name}_df-01" in str(logname)
        for logname in run_dunerc.log_files
    )

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


def test_pms_ps_contains_three_sessions(run_dunerc) -> None:
    """Checks that pms sees the two pms sessions and the us session together."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()
    ps_all = get_ps_table_after_echo(lines, "pms_ps_all_sessions")

    assert ps_all, "Expected a ps table after pms_ps_all_sessions, but found none."

    observed_sessions = {row["session"].strip() for row in ps_all}
    expected_sessions = {
        daq_session_name_1,
        daq_session_name_2,
        conf_dict.config_session_name,
    }
    assert expected_sessions.issubset(observed_sessions), (
        f"Expected sessions {expected_sessions} in pms ps output, got {observed_sessions}."
    )

    assert_rows_have_valid_uuids(ps_all)


def test_pms_ps_session_filter(run_dunerc) -> None:
    """Checks that pms ps -s for session 1 only returns rows from that session."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()
    ps_filtered = get_ps_table_after_echo(lines, "pms_ps_session_1_only")

    assert ps_filtered, (
        "Expected a ps table after pms_ps_session_1_only, but found none."
    )

    observed_sessions = {row["session"].strip() for row in ps_filtered}
    assert observed_sessions == {daq_session_name_1}, (
        f"Expected only session '{daq_session_name_1}' in filtered ps output, got {observed_sessions}."
    )


def test_unknown_log_command(run_dunerc) -> None:
    """Checks that ambiguous logs query without session reports the expected error."""
    test_str = (
        "Bad query for logs: There are more than 1 processes corresponding to the query"
    )
    assert test_str in run_dunerc.completed_processes["pmshell"].stdout


def test_pms_root_controller_logs_scoped(run_dunerc) -> None:
    """Checks scoped root-controller logs from pms for expected format and line count."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()

    marker_idx = require_echo_marker_index(lines, "pms_logs_scoped")
    marker_end_idx = require_echo_marker_index(
        lines, "pms_ps_after_us_terminate", start_idx=marker_idx + 1
    )

    header_idx = require_line_containing(
        lines,
        "root-controller logs",
        error_message="Did not find the 'root-controller logs' header line in stdout.",
        start_idx=marker_idx + 1,
    )
    footer_idx = require_line_containing(
        lines,
        "root-controller end",
        error_message="Did not find the 'root-controller end' footer line in stdout.",
        start_idx=header_idx + 1,
    )

    assert header_idx < marker_end_idx, "Scoped logs header appears after scoped block."
    assert footer_idx < marker_end_idx, "Scoped logs footer appears after scoped block."
    assert footer_idx > header_idx, "Footer appears before header in stdout."

    between = lines[header_idx + 1 : footer_idx]
    assert len(between) == 5, (
        f"Expected exactly 5 lines between header and footer, found {len(between)}.\nBetween:\n"
        + "\n".join(between)
    )

    init_controller_ready_re = re.compile(
        r"drunc\.controller\.core\.init_controller.*Controller ready\s*$"
    )
    matches = [line for line in between if init_controller_ready_re.search(line)]
    assert len(matches) >= 1, (
        "Did not find an init_controller line ending with 'Controller ready' within the 5 lines.\nBetween:\n"
        + "\n".join(between)
    )


def test_us_ps_contains_only_us_session(run_dunerc) -> None:
    """Checks that us ps output only contains the us session."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()
    us_ps = get_ps_table_after_echo(lines, "us_ps_only")

    assert us_ps, "Expected a ps table after us_ps_only, but found none."

    observed_sessions = {row["session"].strip() for row in us_ps}
    assert observed_sessions == {conf_dict.config_session_name}, (
        f"Expected only us session rows in us ps output, got {observed_sessions}."
    )


def test_us_terminated_table_contains_only_us_session(run_dunerc) -> None:
    """Checks that us terminate only reports us session entries in terminated table."""
    lines = strip_ansi(run_dunerc.completed_processes["us"].stdout).splitlines()

    terminate_marker_idx = require_echo_marker_index(lines, "us_terminate")
    terminate_done_idx = require_echo_marker_index(
        lines, "us_terminate_done", start_idx=terminate_marker_idx + 1
    )

    table_start_idx = find_line_index(
        lines,
        lambda line: "Terminated process" in line,
        start_idx=terminate_marker_idx + 1,
    )
    assert table_start_idx is not None, (
        "Could not find terminated process table in us output."
    )
    assert table_start_idx < terminate_done_idx, (
        "Terminated process table appears after us terminate block ended."
    )

    terminated_table = _parse_table_from_index(lines, table_start_idx, _PS_COLUMNS)
    assert terminated_table, (
        "Expected terminated table rows in us output, but found none."
    )

    observed_sessions = {row["session"].strip() for row in terminated_table}
    assert observed_sessions == {conf_dict.config_session_name}, (
        f"Expected only us session rows in terminated table, got {observed_sessions}."
    )


def test_pms_ps_after_us_terminate(run_dunerc) -> None:
    """Checks that us session is gone but pms sessions are still present."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()
    ps_after_us = get_ps_table_after_echo(lines, "pms_ps_after_us_terminate")

    assert ps_after_us, (
        "Expected a ps table after pms_ps_after_us_terminate, but found none."
    )

    observed_sessions = {row["session"].strip() for row in ps_after_us}
    assert conf_dict.config_session_name not in observed_sessions, (
        f"Expected us session '{conf_dict.config_session_name}' to be gone, "
        f"but sessions were {observed_sessions}."
    )
    assert daq_session_name_1 in observed_sessions, (
        f"Expected pms session '{daq_session_name_1}' to still be alive."
    )
    assert daq_session_name_2 in observed_sessions, (
        f"Expected pms session '{daq_session_name_2}' to still be alive."
    )


def test_pms_terminate_all_and_ps_empty(run_dunerc) -> None:
    """Checks that final pms terminate leaves no processes running."""
    lines = strip_ansi(run_dunerc.completed_processes["pmshell"].stdout).splitlines()

    ps_final = get_ps_table_after_echo(lines, "pms_ps_final")
    assert not ps_final, (
        "Expected no running processes after final pms terminate, "
        f"but got {len(ps_final)} entries."
    )

    final_marker_idx = require_echo_marker_index(lines, "pms_ps_final")
    no_process_idx = require_line_containing(
        lines,
        "No processes running",
        error_message="Did not find final 'No processes running' message in pms output.",
        start_idx=final_marker_idx + 1,
    )
    assert no_process_idx > final_marker_idx
