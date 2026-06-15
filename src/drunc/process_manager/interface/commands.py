import getpass

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
from drunc.utils.shell_utils import InterruptedCommand
from drunc.utils.utils import get_logger


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
) -> None:
    log = get_logger("process_manager.shell")
    pm_driver = _pm_driver(obj)
    processes = pm_driver.ps(ProcessQuery(user=user))

    if len(processes.values) > 0:
        click.confirm(
            f"You already have {len(processes.values)} processes running, are you sure you want to boot a session?",
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
            log_level="INFO",  ## Unused anyway!!
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
    log.debug("Terminating")
    result = _pm_driver(obj).terminate()
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Terminated process", False, width=width)
    )  # rich tables require console printing
    obj.delete_driver("controller")


@click.command("kill")
@click.option(
    "-w",
    "--width",
    type=int,
    default=None,
    help="Table width. Default is automatically calculated",
)
@add_query_options()
@click.option(
    "--crash",
    is_flag=True,
    default=False,
    help="Simulate a crash: send SIGKILL without any cleanup, leaving the process manager in an unexpected-death state.",
)
@click.pass_obj
def kill(
    obj: ProcessManagerContext,
    crash: bool,
    session: str | None,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    width: int | None,
) -> None:
    log = get_logger("process_manager.shell")
    query = build_process_query(
        session, name, user, uuid, at_least_one=True, crash=crash
    )
    log.debug(f"Killing with query {query}")
    result = _pm_driver(obj).kill(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Killed process", False, width=width)
    )  # rich tables require console printing


@click.command("flush")
@click.option(
    "-w",
    "--width",
    type=int,
    default=None,
    help="Table width. Default is automatically calculated",
)
@add_query_options()
@click.pass_obj
def flush(
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


@click.command("logs")
@add_query_options()
@click.option(
    "--how-far",
    type=int,
    show_default=True,
    default=100,
    help="How many lines one wants",
)
@click.option("--grep", type=str, default=None)
@click.pass_obj
def logs(
    obj: ProcessManagerContext,
    how_far: int,
    grep: str,
    session: str | None,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
) -> None:
    log = get_logger("process_manager.shell")
    query = build_process_query(session, name, user, uuid, at_least_one=True)
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
def restart(
    obj: ProcessManagerContext,
    session: str | None,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
) -> None:
    log = get_logger("process_manager.shell")
    query = build_process_query(session, name, user, uuid, at_least_one=True)
    log.debug(f"Restarting with query {query}")
    _pm_driver(obj).restart(query)


@click.command("ps")
@add_query_options()
@click.option(
    "-l",
    "--long-format",
    is_flag=True,
    type=bool,
    default=False,
    help="Whether to have a long output",
)
@click.option(
    "-w",
    "--width",
    type=int,
    default=None,
    help="Table width. Default is automatically calculated",
)
@click.pass_obj
def ps(
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
    results = _pm_driver(obj).ps(query)
    if not results:
        return
    obj.print(
        tabulate_process_instance_list(
            results, title="Processes running", long=long_format, width=width
        ),
        overflow="fold",
        soft_wrap=True,
    )
