import getpass
import sys

import click
from druncschema.process_manager_pb2 import ProcessInstance, ProcessQuery
from rich.markup import escape

from drunc.controller.interface.shell_utils import controller_setup
from drunc.exceptions import DruncSetupException
from drunc.process_manager.interface.cli_argument import add_query_options_no_session
from drunc.process_manager.interface.commands import (
    flush_decorators,
    kill_decorators,
    logs_decorators,
    ps_decorators,
)
from drunc.process_manager.utils import (
    build_process_query,
    tabulate_process_instance_list,
)
from drunc.unified_shell.context import UnifiedShellContext, UnifiedShellMode
from drunc.utils.shell_utils import InterruptedCommand, log_pm_cmd
from drunc.utils.utils import get_logger


@click.command("boot")
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    default=None,
    help="Manual override allows for overwriting logs or not, by appending timestamp info. Default (None) is to follow what is used in the initialisation of the unified shell.",
)
@click.option(
    "-cl",
    "--controller-log-level",
    default=None,
    help="Overrides the config-defined log level of the controller",
)
@click.option(
    "--sleep-between-app-boot",
    type=float,
    default=0.1,
    help="Sleep between app boot, in seconds. This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.",
)
@click.pass_obj
def boot(
    obj: UnifiedShellContext,
    override_logs: bool | None,
    controller_log_level: bool | None,
    sleep_between_app_boot: int | float = 0,
) -> None:
    log = get_logger("unified_shell.boot")
    log_pm_cmd(obj)
    session_name = obj.session_name
    user = getpass.getuser()
    processes = obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )

    # Store the number of processes that are expected to be booted with this command, to check later if any processes died immediately after booting.
    expected_booted_processes = 0

    if override_logs is None:
        override_logs_boot = obj.override_logs
    else:
        override_logs_boot = override_logs
    # The run control will validate this in the session manager in the future
    if len(processes.values) > 0:
        log.error(
            f"Cannot boot: session {session_name} already has {len(processes.values)} processes running. "
            "Please terminate the existing session first."
        )
        return

    try:
        results = obj.get_driver("process_manager").boot(
            conf_file=obj.configuration_file,
            conf_id=obj.configuration_id,
            user=user,
            session_name=session_name,
            log_level=controller_log_level,
            override_logs=override_logs_boot,
            sleep_between_app_boot=sleep_between_app_boot,
        )
        expected_booted_processes = sum(1 for _ in results)
        for result in results:
            log.critical(
                f"Booting process: {result.values[0].process_description.metadata.name}"
            )
            if not result:
                break
            log.debug(
                f"'{result.values[0].process_description.metadata.name}' ({result.values[0].uuid.uuid}) started"
            )
    except InterruptedCommand:
        log.warning("Booting interrupted")
        return
    except DruncSetupException as e:
        log.error(e)
        return

    processes = obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )
    if not processes.values:
        log.debug("No processes found after boot - stopping due to previous errors")
        return

    controller_address = obj.get_driver("process_manager").controller_address
    if controller_address:
        log.debug(f"Controller endpoint is '{controller_address}'")
        log.debug("Connecting the unified_shell to the controller endpoint")
        obj.set_controller_driver(controller_address)
        controller_setup(obj, controller_address)

    else:
        log.error("Could not understand where the controller is!")
        return

    # If any processes died immediately, place the controller in error.
    alive_process_count = len(
        [p for p in processes.values if p.status_code == ProcessInstance.RUNNING]
    )

    dead_process_count = expected_booted_processes - alive_process_count

    if (
        not obj.get_driver("controller").status().status.in_error
        and dead_process_count == 0
    ):
        log.info("Booted successfully")
    elif dead_process_count != 0:
        log.error(f"Booted, but {dead_process_count} processes died after booting.")
        # The following line has been commented out as there are issues with the k8s PM
        # booting process, which terminates processes and immediately reboots them. The
        # current cause of this issue is unknown, and has been listed in the issue list.
        # obj.get_driver("controller").to_error()
    elif obj.get_driver("controller").status().status.in_error:
        log.error("Booted, but the top controller is in error")
        if obj.running_mode in [UnifiedShellMode.BATCH, UnifiedShellMode.SEMIBATCH]:
            log.error(
                "Unified shell: Running in batch mode, and because error state is detected, exiting."
            )
            sys.exit(1)


@click.command("terminate")
@click.pass_obj
def terminate(obj: UnifiedShellContext, width: int | None = None) -> None:
    """
    Execute the process manager terminate command, but only do this for the current
    session
    """

    log = get_logger("unified_shell.terminate")
    log_pm_cmd(obj)
    session_query = ProcessQuery(session=obj.session_name)
    log.info(f"Terminating session [green]{obj.session_name}[/]")
    obj.get_driver("process_manager").kill(session_query)

    # As the session is now terminated, we can delete the controller driver, as it is no
    # longer needed.
    obj.delete_driver("controller")


@click.command("ps")
@add_query_options_no_session()
@ps_decorators
@click.pass_obj
def ps(
    obj: UnifiedShellContext,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    long_format: bool,
    width: int | None,
) -> None:
    log_pm_cmd(obj)
    session_query = build_process_query(
        obj.session, name, user, uuid, at_least_one=True, all_processes_by_default=True
    )
    running_processes = obj.get_driver("process_manager").ps(session_query)

    # If there are processes running, tabulate them, otherwise log that there are no
    # processes running.
    if running_processes.values:
        obj.print(
            tabulate_process_instance_list(
                running_processes,
                title=f"Processes running in session {obj.session_name}",
                long=long_format,
                width=width,
            ),
            overflow="fold",
            soft_wrap=True,
        )
    else:
        obj.log.info(f"No processes running in session [green]{obj.session_name}[/]")


@click.command("logs")
@add_query_options_no_session()
@logs_decorators
@click.pass_obj
def logs(
    obj: UnifiedShellContext,
    how_far: int,
    grep: str,
    session: str | None,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
) -> None:
    log_pm_cmd(obj)
    query = build_process_query(
        obj.session,
        name,
        user,
        uuid,
        at_least_one=True,
        all_processes_by_default=False,
    )
    result = obj.get_driver("process_manager").logs(query)
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


@click.command("kill")
@add_query_options_no_session()
@kill_decorators
@click.pass_obj
def kill(
    obj: UnifiedShellContext,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    width: int | None,
    crash: bool,
) -> None:
    log_pm_cmd(obj)
    query = build_process_query(
        obj.session,
        name,
        user,
        uuid,
        at_least_one=True,
        all_processes_by_default=False,
        crash=crash,
    )
    result = obj.get_driver("process_manager").kill(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Killed process", False, width=width)
    )


@click.command("flush")
@add_query_options_no_session()
@flush_decorators
@click.pass_obj
def flush(
    obj: UnifiedShellContext,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    width: int | None,
) -> None:
    log_pm_cmd(obj)
    query = build_process_query(
        obj.session, name, user, uuid, at_least_one=True, all_processes_by_default=False
    )
    result = obj.get_driver("process_manager").flush(query)
    if not result:
        return
    obj.print(
        tabulate_process_instance_list(result, "Flushed process", False, width=width)
    )


@click.command("restart")
@add_query_options_no_session()
@click.pass_obj
def restart(
    obj: UnifiedShellContext,
    name: tuple[str, ...],
    user: str | None,
    uuid: tuple[str, ...],
    width: int | None,
) -> None:
    log_pm_cmd(obj)
    query = build_process_query(
        obj.session, name, user, uuid, at_least_one=True, all_processes_by_default=False
    )
    obj.get_driver("process_manager").restart(query)


@click.command("start-shell")
@click.pass_obj
def start_shell(obj: UnifiedShellContext) -> None:
    """
    Start an interactive shell session.

    This command stops batch mode and enters an interactive shell state,
    allowing you to execute commands interactively.
    """
    log_pm_cmd(obj)
    log = get_logger("unified_shell.start_shell")

    obj.running_mode = UnifiedShellMode.SEMIBATCH
    log.info("Switching to interactive mode...")
