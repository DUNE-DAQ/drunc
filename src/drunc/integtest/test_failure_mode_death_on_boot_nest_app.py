"""
Run a session with a nested segment application dying at the start of boot.
"""

import os
import re

# from datetime import datetime
import integrationtest.data_classes as data_classes

# import integrationtest.log_file_checks as log_file_checks
import pytest
from integ_test_utils import (
    check_file_containing,
    strip_ansi,
)

# Check if drunc is present in the DUNEDAQ_DB_PATH, and if not, skip all tests in this
# file with an appropriate message
present = any(["drunc" in i for i in os.getenv("DUNEDAQ_DB_PATH").split(":")])
if not present:
    pytest.skip(
        "drunc is not present in DUNEDAQ_DB_PATH, skipping drunc integration tests",
        allow_module_level=True,
    )

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
conf_dict.config_session_name = "ft-death-on-boot-nest-app"

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
confgen_arguments = {"test_failure_mode_death_on_boot_nest_app": conf_dict}

# Run these commands in the run control
dunerc_command_list = ["boot"]


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


def test_log_files_are_present(run_dunerc) -> None:
    """Checks that expected process log files exist."""
    generated_log_files = [str(log.name) for log in run_dunerc.log_files]
    for app_name in [
        "ft-root-controller",
        "ft-top-segment-controller",
        "ft-nested-segment-1-controller",
        "ft-nested-segment-1-application",
        "ft-nested-segment-2-controller",
        "ft-nested-segment-2.1-application",
        "ft-top-segment-application",
    ]:
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
            if "ft-nested-segment-2-application" in str(log)
        ),
        None,
    )
    assert simulated_death_app_logfile is not None, (
        "Expected to find a log file for ft-nested-segment-2-application, but did not."
    )

    # Check that the expected boot failure message is in the log file for the
    # application that dies on boot
    app_death_str = ["Simulating death of ft-nested-segment-2-application on boot"]
    line_found = check_file_containing(app_death_str, simulated_death_app_logfile)
    assert line_found == True, (
        "Expected to see the boot failure message in stdout, but did not."
    )


def test_boot_failure_cli(run_dunerc) -> None:
    """
    Checks that the application that dies on boot causes the session to go into an error
    state, and that the expected message is printed to stdout.
    """
    # Check that the session is correctly put in error state if an appliucation dies on
    # boot.
    lines = strip_ansi(run_dunerc.completed_process.stdout).splitlines()
    search_str = "Running in batch mode, and because error state is detected, exiting."
    str_found = any(search_str in line for line in lines)
    assert str_found is True, (
        "Expected to see the boot failure message in stdout, but did not."
    )
