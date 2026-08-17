"""
Run a session with a nested segment application dying at the end of boot.
Check that the session goes into an error state, and that the expected messages are
printed to stdout. The application that dies is ft-top-segment-application.
"""

# from datetime import datetime
import integrationtest.data_classes as data_classes
import integrationtest.utility_functions as utility_functions

# import integrationtest.log_file_checks as log_file_checks
from integ_test_utils import (
    check_file_containing,
    get_ps_table_after_echo,
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
conf_dict.config_session_name = "ft-fsm-cmd-timeout-top-app"
conf_dict.dunerc_cmd_args = ["--no-stop-error-batch-mode"]

# Define the operational environment for this test
conf_dict.op_env = "test"

# Connectivity service configuration
# drunc manages the ConnectivityService with a pre-defined dunedaq configuration
# conf_dict.connsvc_control = data_classes.ConnSvcControl.RUNCONTROL
# Specify connectivity service port (default is 0, a random port is chosen for the
# Connectivity Service)
# conf_dict.connsvc_port = 12345

# Collate tthe drunc config arguments into a dict to pass to the fixture
confgen_arguments = {"test_failure_mode_fsm_cmd_timeout_top_app": conf_dict}

# Run these commands in the run control
dunerc_command_list = """
boot

echo ps-post-boot
ps -w 200

echo status-post-boot
status

echo pre-conf
conf

echo status-post-conf
status
""".split()

timeout_app_name = "ft-top-segment-application"


def test_dunerc_success(run_dunerc, caplog) -> None:
    """
    Checks that the drunc integration command sequence completes successfully without
    any unexpected failures.
    """

    # checks for run control success, problems during pytest setup, etc.
    utility_functions.basic_checks(run_dunerc, caplog, print_test_name=True)


def test_log_files_are_present(run_dunerc) -> None:
    """Checks that expected session log files exist."""
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


def test_all_apps_alive_and_no_initial_error(run_dunerc) -> None:
    """Checks that all expected applications are alive after boot."""
    lines = strip_ansi(run_dunerc.completed_processes["drunc"].stdout).splitlines()

    # Get the ps table
    ps_table_post_boot = get_ps_table_after_echo(lines, "ps-post-boot")
    assert ps_table_post_boot, "Expected ps table after boot, but did not find it."

    # Check that all expected applications are alive after boot
    alive_processes = [
        row["friendly_name"] for row in ps_table_post_boot if row["alive"] == "True"
    ]
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
        assert app_name in alive_processes, (
            f"Expected {app_name} to be alive after boot, but it was not."
        )

    # Get the status table
    status_table_post_boot = get_status_table_after_echo(lines, "status-post-boot")
    assert status_table_post_boot, (
        "Expected status table after boot, but did not find it."
    )

    # Check that the session is not in an error state after boot
    all_application_error_state_query = [
        app["in_error"] for app in status_table_post_boot
    ]
    assert all(state == "No" for state in all_application_error_state_query), (
        "Expected all applications to not be in error state after boot, but found some in error state."
    )


def test_fsm_cmd_timeout_logfile(run_dunerc) -> None:
    """
    Checks that the application that times out on stateful command execution has a
    logfile, and that the defined logfile contains the expected message indicating that
    the stateful command induced delay is being simulated.
    """
    # Retrieve the log file for the application that is configured to timeout on fsm command execution
    simulated_fsm_cmd_delay_logfile = next(
        (log for log in run_dunerc.log_files if timeout_app_name in str(log)),
        None,
    )
    assert simulated_fsm_cmd_delay_logfile is not None, (
        f"Expected to find a log file for {timeout_app_name}, but did not."
    )

    # Check that the expected delay message is present in the log file
    fsm_cmd_delay_str = [
        f"Delaying execution of conf in {timeout_app_name} by 100 seconds"
    ]
    line_found = check_file_containing(
        fsm_cmd_delay_str, simulated_fsm_cmd_delay_logfile
    )
    assert line_found == True, (
        "Expected to see the fsm conf delay message in stdout, but did not."
    )


def test_session_in_error_cli(run_dunerc) -> None:
    """
    Checks that the application that dies on fsm cmd execution causes the session
    to go into an error state, and that the expected message is printed to stdout.
    """
    # Get the status table shown during the command execution
    stdout = run_dunerc.completed_processes["drunc"].stdout
    lines = strip_ansi(stdout).splitlines()
    status_table_post_conf = get_status_table_after_echo(lines, "status-post-conf")

    # Get the root contorller row in the status table
    root_controller_row = get_rows_by_name_from_status_table(
        status_table_post_conf, "ft-root-controller"
    )
    assert root_controller_row, (
        "Expected to find a row for the root controller in the status table, but did not."
    )

    # Check that the root controller did not reach the target state
    assert root_controller_row[0]["substate"] == "propagating-conf", (
        f"Expected root controller substate to be 'propagating-conf', but found '{root_controller_row[0]['substate']}'."
    )

    # Check the state of a segment controller which does not time out reaches the target state
    nested_segment_controller_row = get_rows_by_name_from_status_table(
        status_table_post_conf, "ft-nested-segment-1-controller"
    )
    assert nested_segment_controller_row, (
        "Expected to find a row for the nested segment controller in the status table, but did not."
    )
    assert nested_segment_controller_row[0]["substate"] == "configured", (
        f"Expected nested segment controller state to be 'configured', but found '{nested_segment_controller_row[0]['state']}'."
    )

    # Check the state of a segment application which does not time out reaches the target state
    nested_segment_application_row = get_rows_by_name_from_status_table(
        status_table_post_conf, "ft-nested-segment-1-application"
    )
    assert nested_segment_application_row, (
        "Expected to find a row for the nested segment application in the status table, but did not."
    )
    assert nested_segment_application_row[0]["substate"] == "idle", (
        f"Expected nested segment application state to be 'idle', but found '{nested_segment_application_row[0]['state']}'."
    )

    # Check the stdout for the cmd timeout message
    expected_timeout_message = "The command timed out,"
    assert expected_timeout_message in stdout, (
        "Expected to find the timeout message in stdout, but did not."
    )

    # Checked that this is explicitly logged too
    search_str = "FSM is in error"
    lines = strip_ansi(stdout).splitlines()
    str_found = any(search_str in line for line in lines)
    assert str_found is True, (
        "Expected to see the FSM error report message in stdout, but did not."
    )


def test_suggestion_to_check_logs_is_present(run_dunerc) -> None:
    """
    Checks that the suggestion to check the log files is present in stdout.
    """
    stdout = run_dunerc.completed_processes["drunc"].stdout
    lines = strip_ansi(stdout).splitlines()

    expected_suggestion = f"logs -n {timeout_app_name}"
    suggestion_found = any(expected_suggestion in line for line in lines)
    assert suggestion_found, (
        "Expected to find the suggestion to check log files in stdout, but did not."
    )
