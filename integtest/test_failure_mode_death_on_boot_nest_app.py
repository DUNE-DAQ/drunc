import os
import re

# from datetime import datetime
import integrationtest.data_classes as data_classes

# import integrationtest.log_file_checks as log_file_checks
import pytest
from integ_test_utils import (
    check_file_containing,
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
# * TODO: Note for the next lines, KAB is preparing a PR in integration test that will
# * allow us to remove the relative path naming, and uncomment the line after it.
conf_dict.predefined_config_db = (
    os.path.dirname(__file__) + "/../config/drunc/failure-mode-testing.data.xml"
)
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
confgen_arguments = {"FailureModeTest": conf_dict}

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
    """Checks that expected process-manager log files exist."""
    for app_name in [
        "root-controller",
        "nested-segment-controller",
        "bottom-segment-1-controller",
        "bottom-segment-1-application",
        "bottom-segment-2-controller",
        "bottom-segment-2-application",
        "bottom-segment-2.1-application",
        "nested-segment-application",
    ]:
        assert any(
            f"{run_dunerc.daq_session_name}_{app_name}" in str(logname)
            for logname in run_dunerc.log_files
        )


def test_boot_failure_logfile(run_dunerc) -> None:
    """
    Checks that boot starts the session processes but the targeted application dies on
    boot, with the simulated death message in the logfile.
    """
    # Retrieve the log file for the application that is configured to die on boot
    simulated_death_app_logfile = next(
        (
            log
            for log in run_dunerc.log_files
            if "bottom-segment-2-application" in str(log)
        ),
        None,
    )
    assert simulated_death_app_logfile is not None, (
        "Expected to find a log file for bottom-segment-2-application, but did not."
    )

    # Check that the expected boot failure message is in the log file for the
    # application that dies on boot
    app_death_str = ["Simulating death of bottom-segment-2-application on boot"]
    line_found = check_file_containing(app_death_str, simulated_death_app_logfile)
    assert line_found == True, (
        "Expected to see the boot failure message in stdout, but did not."
    )


def test_boot_failure_cli(run_dunerc) -> None:
    """
    Checks that boot starts the session processes but the targeted application dies on
    boot, with the simulated death message in the stdout and the expected boot failure
    message in stdout.
    """
    # Check that the expected boot failure message is in the
    stdout = run_dunerc.completed_process.stdout
    assert "Booted, but the top controller is in error" in stdout, (
        "Expected to see the boot failure message in stdout, but did not."
    )
