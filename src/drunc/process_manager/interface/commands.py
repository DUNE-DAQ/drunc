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
from drunc.process_manager.utils import tabulate_process_instance_list
from drunc.utils.shell_utils import InterruptedCommand
from drunc.utils.utils import get_logger, run_coroutine


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
@click.argument("session-name", type=str)
@click.argument("configuration-id", type=str)
@click.pass_obj
@run_coroutine
async def boot(
    obj: ProcessManagerContext,
    user: str,
    session_name: str,
    configuration_file: str,
    configuration_id: str,
    override_logs: bool,
) -> None:
    log = get_logger("process_manager.shell")
    processes = await obj.get_driver("process_manager").ps(ProcessQuery(user=user))

    if len(processes.data.values) > 0:
        click.confirm(
            f"You already have {len(processes.data.values)} processes running, are you sure you want to boot a session?",
            abort=True,
        )

    log.debug(
        f"Booting session {session_name} with boot configuration file {configuration_file} and id {configuration_id}, requested by user {user}"
    )
    try:
        results = obj.get_driver("process_manager").boot(
            conf_file=configuration_file,
            conf_id=configuration_id,
            user=user,
            session_name=session_name,
            log_level="INFO",  ## Unused anyway!!
            override_logs=override_logs,
        )
        async for result in results:
            if not result:
                break
            log.debug(
                f"'{result.data.process_description.metadata.name}' ({result.data.uuid.uuid}) process started"
            )
    except InterruptedCommand:
        return
    except Exception as e:
        log.exception(e)
        raise e

    controller_address = obj.get_driver("process_manager").controller_address
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
@run_coroutine
async def dummy_boot(
    obj: ProcessManagerContext,
    user: str,
    n_processes: int,
    sleep: int,
    n_sleeps: int,
    session_name: str,
) -> None:
    log = get_logger("process_manager.shell")
    log.debug(
        f"Running dummy_boot with {n_processes} processes for {sleep} seconds {n_sleeps} times, requested by user {user}"
    )
    try:
        results = obj.get_driver("process_manager").dummy_boot(
            user=user,
            session_name=session_name,
            n_processes=n_processes,
            sleep=sleep,
            n_sleeps=n_sleeps,
        )
        async for result in results:
            if not result:
                break
            log.debug(
                f"'{result.data.process_description.metadata.name}' ({result.data.uuid.uuid}) process started"
            )
    except InterruptedCommand:
        return


@click.command("terminate")
@click.pass_obj
@run_coroutine
async def terminate(obj: ProcessManagerContext) -> None:
    log = get_logger("process_manager.shell")
    log.debug("Terminating")
    result = await obj.get_driver("process_manager").terminate()
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result.data, "Terminated process", False)
    )  # rich tables require console printing

    obj.delete_driver("controller")


@click.command("kill")
@add_query_options(at_least_one=True)
@click.pass_obj
@run_coroutine
async def kill(obj: ProcessManagerContext, query: ProcessQuery) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Killing with query {query}")
    result = await obj.get_driver("process_manager").kill(query=query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result.data, "Killed process", False)
    )  # rich tables require console printing

    obj.delete_driver("controller")


@click.command("flush")
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.pass_obj
@run_coroutine
async def flush(obj: ProcessManagerContext, query: ProcessQuery) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Flushing with query {query}")
    result = await obj.get_driver("process_manager").flush(query=query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result.data, "Flushed process", False)
    )  # rich tables require console printing


@click.command("logs")
@add_query_options(at_least_one=True)
@click.option(
    "--how-far",
    type=int,
    show_default=True,
    default=100,
    help="How many lines one wants",
)
@click.option("--grep", type=str, default=None)
@click.pass_obj
@run_coroutine
async def logs(
    obj: ProcessManagerContext, how_far: int, grep: str, query: ProcessQuery
) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Running logs with query {query}")
    log_req = LogRequest(
        how_far=how_far,
        query=query,
    )

    uuid = None
    async for result in obj.get_driver("process_manager").logs(log_req):
        if not result:
            break

        if uuid is None:
            uuid = result.data.uuid.uuid
            obj.rule(f"[yellow]{uuid}[/yellow] logs")

        line = result.data.line
        if line == "":
            obj.print("")
            continue

        if line[-1] == "\n":
            line = line[:-1]

        if grep is not None and grep not in line:
            continue

        line = escape(line)

        if grep is not None:
            line = line.replace(grep, f"[u]{grep}[/]")

        obj.print(line)
    obj.rule("End")


@click.command("restart")
@add_query_options(at_least_one=True)
@click.pass_obj
@run_coroutine
async def restart(obj: ProcessManagerContext, query: ProcessQuery) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Restarting with query {query}")
    await obj.get_driver("process_manager").restart(query=query)


@click.command("ps")
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.option(
    "-l",
    "--long-format",
    is_flag=True,
    type=bool,
    default=False,
    help="Whether to have a long output",
)
@click.pass_obj
@run_coroutine
async def ps(
    obj: ProcessManagerContext, query: ProcessQuery, long_format: bool
) -> None:
    log = get_logger("process_manager.shell")
    log.debug(f"Running ps with query {query}")
    results = await obj.get_driver("process_manager").ps(query=query)
    if not results:
        return
    obj.print(
        tabulate_process_instance_list(
            results.data, title="Processes running", long=long_format
        )
    )
