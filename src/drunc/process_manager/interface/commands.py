import getpass
from time import sleep

import click
from druncschema.process_manager_pb2 import LogRequest, ProcessQuery
from rich.markup import escape
from rich.panel import Panel

from drunc.process_manager.interface.cli_argument import (
    add_query_options,
    validate_conf_string,
)
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.process_manager.utils import (
    build_process_query,
    tabulate_process_instance_list,
)
from drunc.utils.shell_utils import InterruptedCommand, log_pm_cmd
from drunc.utils.utils import get_logger, resolve_context_peer


def _pm_driver(obj: ProcessManagerContext) -> ProcessManagerDriver:
    driver = obj.get_driver("process_manager")
    if not isinstance(driver, ProcessManagerDriver):
        raise RuntimeError("Process manager driver is not initialized")
    return driver


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
    controller_log_level: bool | None,
) -> None:
    log = get_logger("process_manager.shell")
    log_pm_cmd(obj)
    pm_driver = _pm_driver(obj)
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
        if results is None:
            return
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
    log = get_logger("process_manager.shell")
    log_pm_cmd(obj)
    pm_driver = _pm_driver(obj)
    log.debug(
        f"Running dummy_boot with {n_processes} processes for {sleep} seconds {n_sleeps} times, requested by user {user}"
    )
    try:
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
    log = get_logger("process_manager.shell")
    log_pm_cmd(obj)
    log.debug("Terminating")
    result = _pm_driver(obj).terminate()
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Terminated process", False, width=width)
    )  # rich tables require console printing
    obj.delete_driver("controller")


def kill_decorators(f):
    f = click.pass_obj(f)
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
def kill(obj, query, width):
    log_pm_cmd(obj)
    return kill_impl(obj, query, width)


def kill_impl(
    obj: ProcessManagerContext, query: ProcessQuery, width: int | None
) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Killing with query {query}")
    result = _pm_driver(obj).kill(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Killed process", False, width=width)
    )  # rich tables require console printing


def flush_decorators(f):
    f = click.pass_obj(f)
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
def flush(obj, query, width):
    log_pm_cmd(obj)
    return flush_impl(obj, query, width)


def flush_impl(
    obj: ProcessManagerContext,
    session: str | None,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    width: int | None,
) -> None:
    log = get_logger("process_manager.shell")
    query = build_process_query(
        session, name, user, uuid, at_least_one=False, all_processes_by_default=True
    )
    log.debug(f"Flushing with query {query}")
    result = _pm_driver(obj).flush(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Flushed process", False, width=width)
    )  # rich tables require console printing


def logs_decorators(f):
    f = click.pass_obj(f)
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
def logs(obj, how_far, grep, query):
    log_pm_cmd(obj)
    return logs_impl(obj, how_far, grep, query)


def logs_impl(
    obj: ProcessManagerContext, how_far: int, grep: str, query: ProcessQuery
) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Running logs with query {query}")
    log_req = LogRequest(
        how_far=how_far,
        query=query,
    )

    result = _pm_driver(obj).logs(log_req)
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
@add_query_options()
@click.pass_obj
def restart(obj: ProcessManagerContext, query: ProcessQuery) -> None:
    log_pm_cmd(obj)
    return restart_impl(obj, query)


def restart_impl(obj: ProcessManagerContext, query: ProcessQuery) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Restarting with query {query}")
    _pm_driver(obj).restart(query)


def ps_decorators(f):
    f = click.pass_obj(f)
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
def ps(obj, query, long_format, width):
    log_pm_cmd(obj)
    return ps_impl(obj, query, long_format, width)


def ps_impl(
    obj: ProcessManagerContext,
    session: str | None,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    long_format: bool,
    width: int | None,
) -> None:
    log = get_logger("process_manager.shell")
    query = build_process_query(
        session, name, user, uuid, at_least_one=False, all_processes_by_default=True
    )
    log.debug(f"Running ps with query {query}")
    results = obj.get_driver("process_manager").ps(query)

    # If there are processes running, tabulate them, otherwise log that there are no
    # processes running.
    if results.values:
        obj.print(
            tabulate_process_instance_list(
                results,
                title=f"Processes running in session {obj.session_name}",
                long=long_format,
                width=width,
            ),
            overflow="fold",
            soft_wrap=True,
        )
    else:
        log.info(f"No processes running in session [green]{obj.session_name}[/]")
