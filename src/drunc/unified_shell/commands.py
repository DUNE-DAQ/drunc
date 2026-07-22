import getpass
import sys
from functools import update_wrapper

import click
from druncschema.process_manager_pb2 import ProcessInstance, ProcessQuery

from drunc.controller.interface.shell_utils import controller_setup
from drunc.exceptions import DruncSetupException
from drunc.process_manager.interface.cli_argument import add_query_options_no_session
from drunc.process_manager.interface.commands import (
    flush_decorators,
    flush_impl,
    kill_decorators,
    kill_impl,
    logs_decorators,
    logs_impl,
    ps_decorators,
    ps_impl,
    restart_impl,
)
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.unified_shell.context import UnifiedShellMode
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
    "--sleep-between-app-boot",
    type=float,
    default=0.1,
    help="Sleep between app boot, in seconds. This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.",
)
@click.pass_obj
def boot(
    obj: ProcessManagerContext,
    override_logs: bool | None,
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
            log_level="INFO",  # Unused anyway !!
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
@click.pass_context
def terminate(ctx, obj):
    """
    Execute the process manager terminate command, but only do this for the current
    session
    """

    log = get_logger("unified_shell.terminate")
    log_pm_cmd(obj)
    session_query = ProcessQuery(session=ctx.obj.session_name)
    log.info(f"Terminating session [green]{ctx.obj.session_name}[/]")
    obj.get_driver("process_manager").kill(session_query)

    # As the session is now terminated, we can delete the controller driver, as it is no
    # longer needed.
    obj.delete_driver("controller")


def session_injector(f):
    @click.pass_context
    def wrapper(ctx, *args, **kwargs):
        kwargs["session"] = ctx.obj.session_name
        return ctx.invoke(f, *args, **kwargs)

    return update_wrapper(wrapper, f)


@click.command("ps")
@session_injector
@add_query_options_no_session(at_least_one=True)
@ps_decorators
def ps(obj, query, long_format, width):
    log_pm_cmd(obj)
    return ps_impl(obj, query, long_format, width)


@click.command("logs")
@session_injector
@add_query_options_no_session(at_least_one=True)
@logs_decorators
def logs(obj, how_far, grep, query):
    log_pm_cmd(obj)
    return logs_impl(obj, how_far, grep, query)


@click.command("kill")
@session_injector
@add_query_options_no_session(at_least_one=True)
@kill_decorators
def kill(obj, query, width):
    log_pm_cmd(obj)
    return kill_impl(obj, query, width)


@click.command("flush")
@session_injector
@add_query_options_no_session(at_least_one=True)
@flush_decorators
def flush(obj, query, width):
    log_pm_cmd(obj)
    return flush_impl(obj, query, width)


@click.command("restart")
@session_injector
@add_query_options_no_session(at_least_one=True)
@click.pass_obj
def restart(obj, query):
    log_pm_cmd(obj)
    return restart_impl(obj, query)


@click.command("start-shell")
@click.pass_obj
@click.pass_context
def start_shell(ctx, obj):
    """
    Start an interactive shell session.

    This command stops batch mode and enters an interactive shell state,
    allowing you to execute commands interactively.
    """
    log = get_logger("unified_shell.start_shell")
    log_pm_cmd(obj)

    obj.running_mode = UnifiedShellMode.SEMIBATCH
    log.info("Switching to interactive mode...")
