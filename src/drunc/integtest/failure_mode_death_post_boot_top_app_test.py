"""
Run a session with a nested segment application dying at the end of boot.
Check that the application is marked as dead in the ps table, and disconnected in the
status table, and that the session is in an error state. The application that dies is
ft-top-segment-application.
"""

import os
import re

# from datetime import datetime
import integrationtest.data_classes as data_classes
import integrationtest.utility_functions as utility_functions

# import integrationtest.log_file_checks as log_file_checks
from integ_test_utils import (
    check_file_containing,
    get_ps_table_after_echo,
    get_rows_by_friendly_name_from_ps_table,
    get_rows_by_name_from_status_table,
    get_status_table_after_echo,
    require_drunc,
    strip_ansi,
)

pytestmark = require_drunc

pytest_plugins = "integrationtest.integrationtest_drunc"

check_for_logfile_errors = True

ignored_logfile_problems = {
    "-controller": [
        "Worker with pid \\d+ was terminated due to signal",
        "Connection '.*' not found on the application registry",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

# Point to the drunc config file for this test
conf_dict = data_classes.integtest_params_for_predefined_dunedaq_config()
conf_dict.predefined_config_db = "config/drunc/failure-testing.data.xml"
conf_dict.config_session_name = "ft-death-post-boot-top-app"
conf_dict.dunerc_cmd_args = ["--no-stop-error-batch-mode"]

# Define the operational environment for this test
conf_dict.op_env = "test"

# Connectivity service configuration
# Allow drunc to manage ConnectivityService (default is False, integrationtest manages
# the Connectivity Service)
conf_dict.drunc_connsvc = True
# Specify connectivity service port (default is 0, a random port is chosen for the
# Connectivity Service)
# conf_dict.connsvc_port = 12345

# Collate tthe drunc config arguments into a dict to pass to the fixture
confgen_arguments = {"test_failure_mode_death_post_boot_top_app": conf_dict}

# Run these commands in the run control
dunerc_command_list = """
boot

echo ps-post-boot
ps -w 160

echo status-post-boot
status
""".split()


def test_dunerc_success(run_dunerc, caplog) -> None:
    """Checks that the drunc integration command sequence completes successfully."""

    # checks for run control success, problems during pytest setup, etc.
    utility_functions.basic_checks(run_dunerc, caplog, print_test_name=True)


def test_log_files_are_present(run_dunerc) -> None:
    """Checks that expected process log files exist."""
    generated_log_files = [str(log.name) for log in run_dunerc.log_files]
    for app_name in [
        "ft-root-controller",
        "ft-top-segment-controller",
        "ft-nested-segment-1-controller",
        "ft-nested-segment-1-application",
        "ft-nested-segment-2-controller",
        "ft-nested-segment-2-application",
        "ft-nested-segment-2.1-application",
        "ft-top-segment-application",
    ]:
        print(f"Checking for log file for {app_name}...")
        assert any(
            f"{run_dunerc.daq_session_name}_{app_name}" in logfile
            for logfile in generated_log_files
        )


def test_boot_failure_logfile(run_dunerc) -> None:
    """
    Checks that the application that dies has a logfile, and that the defined logfile
    contains the expected message indicating that the application died on boot.
    """
    # Retrieve the log file for the application that is configured to die on boot
    simulated_death_app_logfile = next(
        (
            log
            for log in run_dunerc.log_files
            if "ft-top-segment-application" in str(log)
        ),
        None,
    )
    assert simulated_death_app_logfile is not None, (
        "Expected to find a log file for ft-top-segment-application, but did not."
    )

    # Check that the expected boot failure message is in the log file for the
    # application that dies on boot
    app_death_str = ["Simulating death of ft-top-segment-application post boot"]
    line_found = check_file_containing(app_death_str, simulated_death_app_logfile)
    assert line_found == True, (
        "Expected to see the boot failure message in stdout, but did not."
    )


def test_expected_log_message_in_terminal(run_dunerc) -> None:
    """
    Checks that the expected message indicating that the application died on boot is
    printed to stdout.
    """
    # Check that the expected boot failure message is in stdout for the application that
    # dies on boot
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()
    search_str = "Booted, but there are disconnected applications/controllers."

    str_found = any(search_str in line for line in lines)
    assert str_found, (
        "Expected to see the misaligned process count record in stdout, but did not."
    )


def test_process_dead_in_ps_table(run_dunerc) -> None:
    """
    Checks that the application that dies on boot is not present in the ps table after
    boot.
    """
    # Check that the application that dies on boot is not present in the ps table after
    # boot.
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()

    ps_table = get_ps_table_after_echo(lines, "ps-post-boot")
    dead_app_name = "ft-top-segment-application"
    ps_table_dead_app_entry = get_rows_by_friendly_name_from_ps_table(
        ps_table, dead_app_name
    )
    assert ps_table_dead_app_entry, (
        f"Expected to see {dead_app_name} in the ps table, but it was not found"
    )
    assert dead_app_name not in ps_table, (
        f"Expected to see {dead_app_name} missing from the ps table, but it was found."
    )
    aliveness_state = ps_table_dead_app_entry[0]["alive"]
    assert aliveness_state == "False", (
        f"Expected to see {dead_app_name} marked as dead in the ps table, but it was not."
    )


def test_process_disconnected_in_status_table(run_dunerc) -> None:
    """
    Checks that the application that dies on boot is marked with a disconnected status
    in the status table after boot.
    """
    # Check that the application that dies on boot is not present in the status table after
    # boot.
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()

    status_table = get_status_table_after_echo(lines, "status-post-boot")
    dead_app_name = "ft-top-segment-application"
    status_table_dead_app_entry = get_rows_by_name_from_status_table(
        status_table, dead_app_name
    )
    assert status_table_dead_app_entry, (
        f"Expected to see {dead_app_name} in the status table, but it was not found"
    )
    assert dead_app_name not in status_table, (
        f"Expected to see {dead_app_name} missing from the status table, but it was found."
    )
    status_table_state = status_table_dead_app_entry[0]["state"]
    assert status_table_state == "disconnected", (
        f"Expected to see {dead_app_name} marked with state 'disconnected' in the status table, but it was not."
    )
    status_table_state = status_table_dead_app_entry[0]["substate"]
    assert status_table_state == "disconnected", (
        f"Expected to see {dead_app_name} marked with substate 'disconnected' in the status table, but it was not."
    )


def test_boot_failure_cli(run_dunerc) -> None:
    """
    Checks that the application that dies on boot causes the session to go into an error
    state, and that the expected message is printed to stdout.
    """
    # Check that the session is correctly put in error state if an appliucation dies on
    # boot.
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()
    search_str = "Booted, but the session is in an error state."
    str_found = any(search_str in line for line in lines)
    assert str_found is True, (
        "Expected to see the boot failure message in stdout, but did not."
    )


def test_fsm_in_error_status_table(run_dunerc) -> None:
    """
    Checks that the session FSM is in an error state after boot if an application dies on
    boot.
    """
    # Check that the session FSM is correctly put in error state if an appliucation dies
    # on boot.
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()

    status_table = get_status_table_after_echo(lines, "status-post-boot")
    root_controller_status = get_rows_by_name_from_status_table(
        status_table, "ft-root-controller"
    )
    assert root_controller_status, (
        "Expected to see the ft-root-controller in the status table, but it was not found"
    )
    error_state = root_controller_status[0]["in_error"]
    assert error_state == "Yes", (
        "Expected to see the session FSM marked as in error in the status table, but it was not."
    )
