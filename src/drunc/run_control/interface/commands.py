"""
Define the click commands for the drunc run control interface.

These are intended primarily for use with the run control shell.
"""

import click
from druncschema.request_response_pb2 import ResponseFlag

from drunc.process_manager.interface.commands import logs_decorators
from drunc.run_control.interface.context import RunControlContext
from drunc.utils.utils import get_logger


@click.command(
    "validate_session",
    help="Run the pre-deployment checks of a new session with the given configuration file path, session id, and name.",
)
@click.argument("process-manager", type=str)
@click.argument("configuration-file", type=str)
@click.argument("session-id", type=str)
@click.argument("session-name", type=str)
@click.pass_obj
def validate_session(
    obj: RunControlContext,
    process_manager: str,
    configuration_file: str,
    session_id: str,
    session_name: str,
) -> None:
    """
    Run the checks before starting the specified session.

    This will include checks against
     - Existing sessions with the same session name
     - Resource availability for the session
     - Configuration file validation
     - Any other checks that are required to ensure the session can be started successfully.

    All entries from validating the session will go into unique log files for later review.

    Args:
        process_manager (str): The process manager to use, either as a technology choice selecting from one 'ssh-standalone', 'ssh-CERN-kafka', 'k8s', or 'k8s-CERN'.
        configuration_file (str): The path to the configuration file, as either an absolute path or a relative path to an instance resolved from the DUNEDAQ_DB_PATH.
        session_id (str): The session ID.
        session_name (str): The session name.

    Returns:
        None
    """
    log = get_logger("run_control.iface.validate_session")
    log.info("Received request to validate a session with the following parameters:")
    log.info(f"\tProcess Manager: {process_manager}")
    log.info(f"\tConfiguration File: {configuration_file}")
    log.info(f"\tSession ID: {session_id}")
    log.info(f"\tSession Name: {session_name}")

    # TODO: Implement the actual validation logic here, including checks for existing sessions, resource availability, and configuration file validation.
    # session_valid = True  # Placeholder for actual validation result

    log.critical("Still requires implementation!")

    return


@click.command(
    "start_session",
    help="Start a new session with the given configuration file path, session id, and name.",
)
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    type=bool,
    default=True,
    help="Override logs, if --no-override-logs filenames have the timestamp of the run.",
)
@click.option(
    "-cl",
    "--controller-log-level",
    type=str,
    default=None,
    help="Overrides the config-defined log level of the controller",
)
@click.option(
    "--sleep-between-app-boot",
    type=float,
    default=None,
    help="Sleep between app boot, in seconds. This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.",
)
@click.argument("process-manager", type=str)
@click.argument("configuration-file", type=str)
@click.argument("session-id", type=str)
@click.argument("session-name", type=str)
@click.pass_obj
def start_session(
    obj: RunControlContext,
    process_manager: str,
    configuration_file: str,
    session_id: str,
    session_name: str,
    override_logs: bool,
    controller_log_level: str | None,
    sleep_between_app_boot: float,
) -> None:
    """
    Start a new session with the given configuration file path, session id, and name.

    Args:
        process_manager (str): The process manager to use, either as a technology choice selecting from one 'ssh-standalone', 'ssh-CERN-kafka', 'k8s', or 'k8s-CERN'.
        configuration_file (str): The path to the configuration file, as either an absolute path or a relative path to an instance resolved from the DUNEDAQ_DB_PATH.
        session_id (str): The session ID.
        session_name (str): The session name.
        override_logs (bool): Whether to override logs.
        controller_log_level (str | None): Overrides the config-defined log level of the controller.
        sleep_between_app_boot (float): Sleep between app boot, in seconds.

    Returns:
        dict[str, str]: A dictionary containing the endpoint addresses.
    """
    log = get_logger("run_control.iface.start_session")
    log.info("Sending request to start session with the following parameters:")
    log.info(f"\tProcess Manager: {process_manager}")
    log.info(f"\tConfiguration File: {configuration_file}")
    log.info(f"\tSession ID: {session_id}")
    log.info(f"\tSession Name: {session_name}")

    # Send the request to the run control server.
    obj.get_driver("run_control").start_session(
        process_manager=process_manager,
        path_to_configuration_file=configuration_file,
        session_id=session_id,
        session_name=session_name,
        override_logs=override_logs,
        controller_log_level=controller_log_level,
        sleep_between_app_boot=sleep_between_app_boot,
    )
    log.critical("Still requires implementation!")
    return


@click.command("end_session", help="End an existing session indexed by name.")
@click.argument("session-name", type=str)
@click.pass_obj
def end_session(
    obj: RunControlContext,
    session_name: str,
) -> None:
    """
    Terminate a running session.

    Run the following
     - Cleanup procedures for the session
     - Release any resources held by the session
     - Log the termination of the session for auditing purposes
     - Any other necessary steps to ensure the session is ended cleanly.

    Args:
        session_name (str): The session name.

    Returns:
        None
    """
    log = get_logger("run_control.iface.end_session")
    log.info(f"Received request to end session {session_name}.")
    obj.get_driver("run_control").end_session(session_name=session_name)
    log.critical("Still requires implementation!")
    return


@click.command("log", help="Send a message to be logged on the run control server.")
@click.argument("msg", type=str)
@click.option(
    "-l",
    "--log-level",
    type=str,
    default="INFO",
    help="Severity of the message to log on the run control server",
)
@click.pass_obj
def log_on_server(obj: RunControlContext, msg: str, log_level: str) -> None:
    """
    Send a message to be logged on the run control server.

    Args:
        msg: The message to log.
        log_level: The severity of the message to log on the run control server.

    Returns:
        None
    """
    log = get_logger("run_control.iface.log")
    log.info("Received request to log.")

    # Send the request to the run control server.
    result = obj.get_driver("run_control").log_on_server(msg=msg, log_level=log_level)

    if result == ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED:
        log.error(f"The requested level {log_level} is not recognised, did not log.")
    return


@click.command("logs", help="Retrieve the logs of the run control server.")
@logs_decorators
def logs(obj: RunControlContext, grep: str, how_far: int) -> None:
    """
    Retrieve the contents of the run control server logs.

    Args:
        grep: A string to filter the logs by.
        how_far: How many lines of logs to retrieve.

    Returns:
        None
    """
    # TODO - extend this to get the run control logs per session too!
    log = get_logger("run_control.iface.logs")
    log.info("Received request to logs.")

    # Send the request to the run control server.
    obj.get_driver("run_control").logs(grep=grep, how_far=how_far)
    return
