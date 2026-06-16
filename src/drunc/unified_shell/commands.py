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

    # If the session applications are not found on the connectivity serivce, then the
    # session is not booted correctly. This is a critical error, and the controller is
    # put into error state.
    ps_response = obj.get_driver("process_manager").ps(
        ProcessQuery(session=session_name)
    )
    ps_process_count = len(ps_response.values)

    status_response = obj.get_driver("controller").status()
    status_process_count = count_processes_in_status_response(status_response)

    # Local connectivity serivces are not reported in the status table, but they should
    # be. Increment the status_process_count by 1 if using the LCS.
    # TODO: Remove this once the LCS is reported in the status table.
    if obj.session_uses_local_connectivity_service:
        status_process_count += 1

    if ps_process_count != status_process_count:
        time.sleep(1)
        log.error(
            f"Booted, but the number of processes found in the connectivity service "
            f"({ps_process_count}) does not match the number of processes found in the "
            f"process manager ({status_process_count}). Please check the relevant logs "
            "for more information."
        )
        log.critical("Getting the controller driver test")
        obj.get_driver("controller")
        log.critical("Status test")
        obj.get_driver("controller").status()
        log.critical("To error test")
        obj.get_driver("controller").to_error()
        log.critical("COMPLETE")
        return

    # Check if session booted correctly, if not put it in error state
    log.warning("Getting the session states")
    session_states = get_all_states(status_response)
    if "disconnected" in session_states:
        time.sleep(1)
        log.error(
            "Booted, but there are disconnected applications/controllers. Please check "
            "the relevant logs for more information."
        )
        status_response = obj.get_driver("controller").status()
        log.critical(f"{status_response=}")
        obj.get_driver("controller").to_error()
        log.critical("TEST")
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
    elif (
        obj.get_driver("controller").status().status.in_error
        and not obj.no_stop_error_batch_mode
    ):
        log.error("Booted, but the top controller is in error")
        if obj.running_mode in [UnifiedShellMode.BATCH, UnifiedShellMode.SEMIBATCH]:
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
