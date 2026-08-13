import getpass
from time import sleep
from typing import Callable

import click
from click.decorators import FC
from druncschema.process_manager_pb2 import LogRequest, ProcessQuery
from rich.markup import escape
from rich.panel import Panel

from drunc.process_manager.interface.cli_argument import (
    add_query_options,
    validate_conf_string,
)
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.process_manager.utils import tabulate_process_instance_list
from drunc.unified_shell.context import UnifiedShellContext
from drunc.utils.shell_utils import InterruptedCommand, log_pm_cmd
from drunc.utils.utils import get_logger, resolve_context_peer


@click.command("boot")
@click.option(
    "-u",
    "--user",
    type=str,
    default=getpass.getuser(),
    help="Select the process of a particular user (default $USER)",
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
    default=None,
    help="Overrides the config-defined log level of the controller",
)
@click.argument("configuration-file", type=str, callback=validate_conf_string)
@click.argument("configuration-id", type=str)
@click.argument("session-name", type=str)
@click.pass_obj
def boot(
    obj: ProcessManagerContext,
    user: str,
    session_name: str,
    configuration_file: str,
    configuration_id: str,
    override_logs: bool,
    controller_log_level: str | None,
) -> None:
    """
    Boot a session with the given configuration file and configuration ID.

    Calls the process manager driver to boot a session with the specified parameters.
    If there are already running processes for the given session name, it prompts the
    user for confirmation before proceeding.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        user: The user for whom the session is being booted.
        session_name: The name of the session to be booted.
        configuration_file: The path to the configuration file to be used for booting
            the session.
        configuration_id: The ID of the configuration to be used for booting the
            session.
        override_logs: A boolean indicating whether to override existing logs or not.
        controller_log_level: An optional log level to override the configuration
            defined log level of the controller.

    Returns:
        None

    Raises:
        InterruptedCommand: If the command is interrupted by the user.
        Exception: If any other exception occurs during the boot process.
    """
    log = get_logger("process_manager.shell")
    log_pm_cmd(obj)
    pm_driver = obj.get_pm_driver()
    processes = pm_driver.ps(ProcessQuery(user=user, session=session_name))

    # The run control will validate this in the session manager in the future
    if len(processes.values) > 0:
        click.confirm(
            f"You already have {len(processes.values)} processes running for {session_name}, are you sure you want to boot a session?",
            abort=True,
        )

    log.debug(
        f"Booting session {session_name} with boot configuration file {configuration_file} and id {configuration_id}, requested by user {user}"
    )
    try:
        results = pm_driver.boot(
            conf_file=configuration_file,
            conf_id=configuration_id,
            user=user,
            session_name=session_name,
            log_level=controller_log_level,
            override_logs=override_logs,
        )
        for result in results:
            if not result:
                break
            log.debug(
                f"'{result.values[0].process_description.metadata.name}' ({result.values[0].uuid.uuid}) process started"
            )
    except InterruptedCommand:
        return
    except Exception as e:
        log.exception(e)
        raise e

    controller_address = pm_driver.controller_address
    controller_address = resolve_context_peer(controller_address)
    if controller_address:
        obj.print(
            Panel(
                f"Controller endpoint: '{controller_address}', point your 'drunc-controller-shell' to it.",
                padding=(2, 6),
                style="violet",
                border_style="violet",
            ),
            justify="center",
        )  # rich tables require console printing
    else:
        log.error(
            "Could not understand where the controller is! You can look at the logs of the controller to see its address"
        )
        return


@click.command("dummy_boot")
@click.option(
    "-u",
    "--user",
    type=str,
    default=getpass.getuser(),
    help="Select the process of a particular user (default $USER)",
)
@click.option(
    "-n",
    "--n-processes",
    type=int,
    default=1,
    help="Select the number of dummy processes to boot (default 1)",
)
@click.option(
    "-s",
    "--sleep",
    type=int,
    default=10,
    help="Select the timeout duration in seconds (default 30)",
)
@click.option(
    "--n_sleeps", type=int, default=6, help="Select the number of timeouts (default 5)"
)
@click.argument("session-name", type=str)
@click.pass_obj
def dummy_boot(
    obj: ProcessManagerContext,
    user: str,
    n_processes: int,
    sleep: int,
    n_sleeps: int,
    session_name: str,
) -> None:
    """
    Boot a session with the given number of dummy processes.

    The dummy processes will sleep for the specified duration and number of times. This
    command used for testing the process manager without running actual processes.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        user: The user for whom the session is being booted.
        n_processes: The number of dummy processes to boot.
        sleep: The duration in seconds for which each dummy process will sleep.
        n_sleeps: The number of times each dummy process will sleep.
        session_name: The name of the session to be booted.

    Returns:
        None

    Raises:
        InterruptedCommand: If the command is interrupted by the user.
    """
    log = get_logger("process_manager.shell")
    log_pm_cmd(obj)
    log.debug(
        f"Running dummy_boot with {n_processes} processes for {sleep} seconds {n_sleeps} times, requested by user {user}"
    )
    try:
        pm_driver = obj.get_pm_driver()
        results = pm_driver.dummy_boot(
            user=user,
            session_name=session_name,
            n_processes=n_processes,
            sleep=sleep,
            n_sleeps=n_sleeps,
        )
        for result in results:
            if not result:
                break
            log.debug(
                f"'{result.values[0].process_description.metadata.name}' ({result.values[0].uuid.uuid}) process started"
            )
    except InterruptedCommand:
        return


@click.command("wait")
@click.argument("sleep_time", type=int, default=1)
@click.pass_obj
def wait(obj: ProcessManagerContext, sleep_time: int) -> None:
    log = get_logger("process_manager.wait")
    log.info(f"Command [green]wait[/green] running for {sleep_time} seconds.")
    sleep(sleep_time)  # seconds
    log.info(f"Command [green]wait[/green] ran for {sleep_time} seconds.")


@click.command("terminate")
@click.option(
    "-w",
    "--width",
    type=int,
    default=None,
    help="Table width. Default is automatically calculated",
)
@click.pass_obj
def terminate(obj: ProcessManagerContext, width: int | None) -> None:
    """
    Terminate the process manager and all its managed processes.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        width: The width of the table to display the terminated processes. If None, the
            width will be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the termination process.
    """
    log = get_logger("process_manager.shell")
    log_pm_cmd(obj)
    log.debug("Terminating")
    pm_driver = obj.get_pm_driver()
    result = pm_driver.terminate()
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Terminated process", False, width=width)
    )  # rich tables require console printing
    obj.delete_driver("controller")


def kill_decorators(f: FC) -> Callable[[FC], FC]:
    """
    Decorator function to add click options to the 'kill' command.

    Args:
        f: The function to be decorated.

    Returns:
        The decorated function with added options for the 'kill' command.

    Raises:
        None
    """
    f = click.option(
        "-w",
        "--width",
        type=int,
        default=None,
        help="Table width. Default is automatically calculated",
    )(f)
    f = click.option(
        "--crash",
        is_flag=True,
        default=False,
        help="Simulate a crash: send SIGKILL without any cleanup, leaving the process manager in an unexpected-death state.",
    )(f)
    return f


@click.command("kill")
@add_query_options(at_least_one=True)
@kill_decorators
@click.pass_obj
def kill(obj: ProcessManagerContext, query: ProcessQuery, width: int | None) -> None:
    """
    Kill processes matching the given query.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be killed.
        width: The width of the table to display the killed processes. If None, the
            width will be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the kill process.
    """
    log_pm_cmd(obj)
    return kill_impl(obj, query, width)


def kill_impl(
    obj: ProcessManagerContext | UnifiedShellContext,
    query: ProcessQuery,
    width: int | None,
) -> None:
    """
    Implementation of the 'kill' command.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be killed.
        width: The width of the table to display the killed processes. If None, the
            width will be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the kill process.
    """
    log = get_logger("process_manager.shell")
    log.debug(f"Killing with query {query}")
    pm_driver = obj.get_pm_driver()
    result = pm_driver.kill(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Killed process", False, width=width)
    )  # rich tables require console printing


def flush_decorators(f: FC) -> Callable[[FC], FC]:
    """
    Decorator function to add click options to the 'flush' command.

    Args:
        f: The function to be decorated.

    Returns:
        The decorated function with added options for the 'flush' command.

    Raises:
        None
    """
    f = click.option(
        "-w",
        "--width",
        type=int,
        default=None,
        help="Table width. Default is automatically calculated",
    )(f)
    return f


@click.command("flush")
@add_query_options(at_least_one=False, all_processes_by_default=True)
@flush_decorators
@click.pass_obj
def flush(obj: ProcessManagerContext, query: ProcessQuery, width: int | None) -> None:
    """
    Flush processes matching the given query.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be flushed.
        width: The width of the table to display the flushed processes. If None, the
            width will be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the flush process.
    """
    log_pm_cmd(obj)
    return flush_impl(obj, query, width)


def flush_impl(
    obj: ProcessManagerContext | UnifiedShellContext,
    query: ProcessQuery,
    width: int | None,
) -> None:
    """
    Implementation of the 'flush' command.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be flushed.
        width: The width of the table to display the flushed processes. If None, the
            width will be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the flush process.
    """
    log = get_logger("process_manager.shell")
    log.debug(f"Flushing with query {query}")
    pm_driver = obj.get_pm_driver()
    result = pm_driver.flush(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Flushed process", False, width=width)
    )  # rich tables require console printing


def logs_decorators(f: FC) -> Callable[[FC], FC]:
    """
    Decorator function to add click options to the 'logs' command.

    Args:
        f: The function to be decorated.

    Returns:
        The decorated function with added options for the 'logs' command.

    Raises:
        None
    """
    f = click.option("--grep", type=str, default=None)(f)
    f = click.option(
        "--how-far",
        type=int,
        show_default=True,
        default=100,
        help="How many lines one wants",
    )(f)
    return f


@click.command("logs")
@add_query_options(at_least_one=True)
@logs_decorators
@click.pass_obj
def logs(
    obj: ProcessManagerContext, how_far: int, grep: str | None, query: ProcessQuery
) -> None:
    """
    Display logs for processes matching the given query.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        how_far: The number of lines of logs to display.
        grep: A string to filter the logs. Only lines containing this string will be
            displayed. If None, all lines will be displayed.
        query: The query to match processes whose logs are to be displayed.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the log retrieval process.
    """
    log_pm_cmd(obj)
    return logs_impl(obj, how_far, grep, query)


def logs_impl(
    obj: ProcessManagerContext | UnifiedShellContext,
    how_far: int,
    grep: str | None,
    query: ProcessQuery,
) -> None:
    """
    Implementation of the 'logs' command.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        how_far: The number of lines of logs to display.
        grep: A string to filter the logs. Only lines containing this string will be
            displayed. If None, all lines will be displayed.
        query: The query to match processes whose logs are to be displayed.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the log retrieval process.
    """
    log = get_logger("process_manager.shell")
    log.debug(f"Running logs with query {query}")
    log_req = LogRequest(
        how_far=how_far,
        query=query,
    )

    pm_driver = obj.get_pm_driver()
    result = pm_driver.logs(log_req)
    if result is None:
        return

    display_name = result.name or result.uuid.uuid or ""
    if result.name is not None:
        obj.rule(f"[yellow]{display_name}[/yellow] logs")

    for line in result.lines:
        if not line.strip():  # keep empty lines for visual clarity
            obj.print("")
            continue

        line = line.rstrip("\n")  # remove trailing newline

        if grep is not None and grep not in line:
            continue

        line = escape(line)

        if grep is not None:
            line = line.replace(grep, f"[u]{grep}[/]")

        obj.print(line, soft_wrap=True)
    if result.name is not None:
        obj.rule(f"[yellow]{display_name}[/yellow] end")


@click.command("restart")
@add_query_options(at_least_one=True)
@click.pass_obj
def restart(obj: ProcessManagerContext, query: ProcessQuery) -> None:
    """
    Restart processes matching the given query.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be restarted.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the restart process.
    """
    log_pm_cmd(obj)
    return restart_impl(obj, query)


def restart_impl(
    obj: ProcessManagerContext | UnifiedShellContext, query: ProcessQuery
) -> None:
    """
    Implementation of the 'restart' command.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be restarted.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the restart process.
    """
    log = get_logger("process_manager.shell")
    log.debug(f"Restarting with query {query}")
    pm_driver = obj.get_pm_driver()
    pm_driver.restart(query)


def ps_decorators(f: FC) -> Callable[[FC], FC]:
    """
    Decorator function to add click options to the 'ps' command.

    Args:
        f: The function to be decorated.

    Returns:
        The decorated function with added options for the 'ps' command.

    Raises:
        None
    """
    f = click.option(
        "-w",
        "--width",
        type=int,
        default=None,
        help="Table width. Default is automatically calculated",
    )(f)
    f = click.option(
        "-l",
        "--long-format",
        is_flag=True,
        type=bool,
        default=False,
        help="Whether to have a long output",
    )(f)

    return f


@click.command("ps")
@add_query_options(at_least_one=False, all_processes_by_default=True)
@ps_decorators
@click.pass_obj
def ps(
    obj: ProcessManagerContext,
    query: ProcessQuery,
    long_format: bool,
    width: int | None,
) -> None:
    """
    Display processes matching the given query.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be displayed.
        long_format: A boolean indicating whether to display the processes in long
            format.
        width: The width of the table to display the processes. If None, the width will
            be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the process retrieval or display.
    """
    log_pm_cmd(obj)
    return ps_impl(obj, query, long_format, width)


def ps_impl(
    obj: ProcessManagerContext | UnifiedShellContext,
    query: ProcessQuery,
    long_format: bool,
    width: int | None,
) -> None:
    """
    Implementation of the 'ps' command.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        query: The query to match processes to be displayed.
        long_format: A boolean indicating whether to display the processes in long
            format.
        width: The width of the table to display the processes. If None, the width will
            be automatically calculated.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the process retrieval or display.
    """
    log = get_logger("process_manager.shell")
    log.debug(f"Running ps with query {query}")
    pm_driver = obj.get_pm_driver()
    results = pm_driver.ps(query)

    # Inject session name if exits
    ## Session name can come from either the process manager shell with --session
    ## Or in the unified shell, where the session name is injected automatically

    session_name = (
        getattr(query, "session", None) or getattr(obj, "session_name", None) or ""
    )
    title = f"Processes running{f' in session {session_name}' if session_name else ''}"
    # If there are processes running, tabulate them, otherwise log that there are no
    # processes running.
    if results.values:
        obj.print(
            tabulate_process_instance_list(
                results,
                title=title,
                long=long_format,
                width=width,
            ),
            overflow="fold",
            soft_wrap=True,
        )
    else:
        if session_name:
            log.info(f"No processes running in session [green]{session_name}[/]")
        else:
            log.info("No processes running")


@click.command("log")
@click.argument("text", required=True)
@click.option(
    "-s",
    "--severity",
    type=str,
    default="INFO",
    help=(
        "Severity level of the log message (default INFO). Options: DEBUG, INFO, "
        "WARNING, ERROR, CRITICAL"
    ),
)
@click.pass_obj
def log_on_server(obj: ProcessManagerContext, text: str, severity: str) -> None:
    """
    Log a message on the server with the specified severity level.

    Args:
        obj: The context object containing the process manager driver and other relevant
            information.
        text: The log message to be sent to the server.
        severity: The severity level of the log message. Options are DEBUG, INFO,
            WARNING, ERROR, CRITICAL. Default is INFO.

    Returns:
        None

    Raises:
        Exception: If any exception occurs during the logging process.
    """
    pm_driver = obj.get_pm_driver()
    pm_driver.log_on_server(text=text, severity=severity)
