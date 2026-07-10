import getpass
import sys
import time

import click
from druncschema.process_manager_pb2 import ProcessInstance, ProcessQuery

from drunc.controller.interface.shell_utils import controller_setup
from drunc.controller.utils import count_processes_in_status_response, get_all_states
from drunc.exceptions import DruncSetupException
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.unified_shell.context import UnifiedShellMode
from drunc.utils.shell_utils import InterruptedCommand
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

    # Determine whether the session should be placed into an error state
    put_in_error_state: bool = False

    # If the session applications are not found on the connectivity serivce, then the
    # session is not booted correctly. This is a critical error, the user should be
    # informed, and the session should be placed in error state.
    ps_response = obj.get_driver("process_manager").ps(
        ProcessQuery(session=session_name)
    )
    ps_process_count = len(ps_response.values)

    status_response = obj.get_driver("controller").status()
    status_process_count = count_processes_in_status_response(status_response)

    # Local connectivity serivces are not reported in the status table, but they should
    # be. Increment the status_process_count by 1 if using the LCS.
    # TODO: Remove this once the LCS is reported in the status table (issue 745).
    if obj.session_uses_local_connectivity_service:
        status_process_count += 1

    if ps_process_count != status_process_count:
        time.sleep(1)
        log.info(  # TODO - once issue 793 is resolved, this should be a log.error
            f"Booted, but the number of processes registered with the process manager "
            f"({ps_process_count}) does not match the number of processes registered "
            f"with the top segment (root) controller ({status_process_count}). Use the "
            "[yellow]ps[/] command to determine which applications did not correctly "
            "register themselves on the connectivity service by comparing against the "
            "status table, and the [yellow]logs[/] command to find out more about this "
            "failure."
        )
        # TODO: Uncomment this once the cause of inconsistent status table printing is
        # understood (issue 793)
        # put_in_error_state = True

    # Check if session booted correctly, if not put it in error state
    session_states = get_all_states(status_response)
    if "disconnected" in session_states:
        time.sleep(1)
        log.error(
            "Booted, but there are disconnected applications/controllers. Use the "
            "[yellow]logs[/] command to find out more about this failure."
        )
        put_in_error_state = True

    # If any processes died immediately, place the controller in error.
    alive_process_count = len(
        [p for p in processes.values if p.status_code == ProcessInstance.RUNNING]
    )
    dead_process_count = expected_booted_processes - alive_process_count
    if dead_process_count > 0:
        time.sleep(1)
        log.error(
            f"Booted, but {dead_process_count} processes died. Use the [yellow]ps[/] "
            "command to find out which applications are dead, and [yellow]logs[/] "
            "command to find out more about this failure on a per-application basis."
        )
        put_in_error_state = True

    # Check if there is or should be an error state. If not, then the boot was
    # successful and we can return, otherwise, we will log the error and place the
    # session in an error state if required.
    in_error_state = obj.get_driver("controller").status().status.in_error
    if not in_error_state and not put_in_error_state:
        log.info("Booted successfully")
        return

    # An error state has been detected, or should be placed. Log the error and place the
    # session in an error state if required.
    log.info(
        "Booted, but the session is in an error state. Use the [yellow]status[/] "
        "command to find out more about this failure, and check the logs of the "
        "applications that are in an error state with the [yellow]logs[/] command."
    )
    if put_in_error_state and not in_error_state:
        log.error("Placing the session into an error state due to boot issues")
        obj.get_driver("controller").to_error()
        in_error_state = obj.get_driver("controller").status().status.in_error

    # If the unified shell is running in batch or semibatch mode, exit with a non-zero
    # exit code unless bypassed with the --no-stop-error-batch-mode option in the
    # unified shell.
    if (
        in_error_state
        and obj.running_mode in [UnifiedShellMode.BATCH, UnifiedShellMode.SEMIBATCH]
        and not obj.no_stop_error_batch_mode
    ):
        log.error(
            "Running in batch mode, and because error state is detected, exiting."
        )
        sys.exit(1)


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

    obj.running_mode = UnifiedShellMode.SEMIBATCH
    log.info("Switching to interactive mode...")
