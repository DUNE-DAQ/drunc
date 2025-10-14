import getpass
import logging
import multiprocessing as mp
import os
import sys
from time import sleep
from urllib.parse import urlparse

import click
import click_shell
import conffwk
from druncschema.process_manager_pb2 import ProcessQuery

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.controller.configuration import ControllerConfHandler
from drunc.controller.interface.commands import (
    connect,
    disconnect,
    exclude,
    expert_command,
    include,
    recompute_status,
    status,
    surrender_control,
    take_control,
    to_error,
    wait,
    who_am_i,
    who_is_in_charge,
)
from drunc.controller.interface.shell_utils import generate_fsm_command
from drunc.controller.stateful_node import StatefulNode
from drunc.exceptions import DruncSetupException
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.utils import convert_fsm_transition
from drunc.process_manager.configuration import (
    ProcessManagerTypes,
    get_process_manager_configuration,
    validate_pm_config,
)
from drunc.process_manager.interface.commands import (
    flush,
    kill,
    logs,
    ps,
    restart,
    terminate,
)
from drunc.process_manager.interface.process_manager import run_pm
from drunc.process_manager.utils import get_pm_type_from_name, validate_k8s_session_name
from drunc.unified_shell.commands import boot
from drunc.unified_shell.shell_utils import generate_fsm_sequence_command
from drunc.utils.configuration import ConfTypes, OKSKey
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    create_logger_handler,
    format_name_for_cli,
    get_logger,
    ignore_sigint_sighandler,
    log_levels,
    pid_info_str,
    resolve_localhost_and_127_ip_to_network_ip,
    setup_root_logger,
)


@click_shell.shell(
    prompt="drunc-unified-shell > ",
    chain=True,
    hist_file=os.path.expanduser("~") + "/.drunc-unified-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.argument("process-manager", type=str, nargs=1)
@click.argument("configuration-file", type=str, nargs=1)
@click.argument("configuration-id", type=str, nargs=1)
@click.argument("session-name", type=str, nargs=1)
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    type=bool,
    default=True,
    help="Override logs, if --no-override-logs filenames have the timestamp of the run.",
)  # For production, change default to false/remove it
@click.option(
    "-lp",
    "--log-path",
    type=str,
    default=None,
    help="Log path of process_manager logs.",
)
@click.option(
    "-s",
    "--safe-mode",
    is_flag=True,
    default=False,
    help=(
        "Enable safe mode. This will force attempting to return the state to initial "
        "before running terminate. This is not particularly useful for testing but "
        "will be useful for hardware operations."
    ),
)  # For production, change default to true/remove it
@click.pass_context
def unified_shell(
    ctx,
    process_manager: str,
    configuration_file: str,
    configuration_id: str,
    session_name: str,
    log_level: str,
    override_logs: bool,
    log_path: str,
    safe_mode: bool,
) -> None:
    # Set up the drunc and unified_shell loggers
    setup_root_logger(log_level)
    unified_shell_log = get_logger("unified_shell")
    create_logger_handler(rich_handler=True)

    unified_shell_log.debug("Set up [green]unified_shell[/green] logger")
    unified_shell_log.debug(pid_info_str())

    process_manager_url = urlparse(process_manager)
    if (
        process_manager_url.scheme != "grpc"
    ):  # slightly hacky to see if the process manager is an address
        internal_pm = True
    else:
        internal_pm = False

    if get_pm_type_from_name(
        process_manager
    ) == ProcessManagerTypes.K8s and not validate_k8s_session_name(session_name):
        unified_shell_log.error(
            f"[red]Invalid session/namespace name [bold]({session_name})[/bold][/red]. "
            "Must match RFC1123 label: lowercase alphanumeric or '-', start/end with "
            "alphanumeric, max 63 chars."
        )
        sys.exit(1)

    ctx.obj.configuration_file = f"oksconflibs:{configuration_file}"
    ctx.obj.configuration_id = configuration_id
    ctx.obj.session_name = session_name

    db = conffwk.Configuration(ctx.obj.configuration_file)
    session_dal = db.get_dal(class_name="Session", uid=ctx.obj.configuration_id)
    app_log_path = session_dal.log_path

    connectivity_service_address = f"{session_dal.connectivity_service.host}:{session_dal.connectivity_service.service.port}"

    unified_shell_log.info(
        f'Setting up to use [green]process_manager[/green] with configuration [green]{process_manager}[/green] and [green]configuration id "{configuration_id}"[/green] from [green]{ctx.obj.configuration_file}[/green]'
    )

    if internal_pm:
        unified_shell_log.debug(
            f"Spawning [green]process_manager[/green] with configuration {process_manager}"
        )
        # Check if process_manager is a packaged config
        process_manager_conf_file = get_process_manager_configuration(process_manager)

        if not validate_pm_config(process_manager_conf_file):
            unified_shell_log.error(
                "Process manager configuration validation failed. Exiting."
            )
            sys.exit(1)

        unified_shell_log.info("Starting process manager")
        ready_event = mp.Event()
        port = mp.Value("i", 0)

        unified_shell_log.debug(
            "Starting [green]process_manager[/green] as separate process"
        )
        ctx.obj.pm_process = mp.Process(
            target=run_pm,
            kwargs={
                "pm_conf": process_manager_conf_file,
                "pm_address": "localhost:0",
                "override_logs": override_logs,
                "log_level": log_level,
                "log_path": app_log_path,
                "ready_event": ready_event,
                "signal_handler": ignore_sigint_sighandler,
                # sigint gets sent to the PM, so we need to ignore it, otherwise everytime the user ctrl-c on the shell, the PM goes down
                "generated_port": port,
            },
        )
        ctx.obj.pm_process.start()
        unified_shell_log.debug("[green]process_manager[/green] started")

        for _ in range(100):
            if ready_event.is_set():
                break
            sleep(0.1)
        if not ready_event.is_set():
            raise DruncSetupException(
                "[green]process_manager[/green] [red]did not start in time[/red]"
            )
        process_manager_address = resolve_localhost_and_127_ip_to_network_ip(
            f"localhost:{port.value}"
        )

    else:  # user provided an address
        process_manager_address = process_manager.replace(
            "grpc://", ""
        )  # remove the grpc scheme
        unified_shell_log.info(
            f"[green]unified_shell[/green] connected to the [green]process_manager[/green] ([green]{process_manager}[/green]) at address [green]{process_manager_address}[/green]"
        )

    unified_shell_log.debug(
        f"[green]process_manager[/green] started, communicating through address [green]{process_manager_address}[/green]"
    )
    ctx.obj.reset(address_pm=process_manager_address)

    desc = None
    try:
        unified_shell_log.debug("Running [green]describe[/green]")
        try:
            desc = ctx.obj.get_driver().describe()
        except Exception as e:
            unified_shell_log.error(
                f"[red]Could not connect to the process manager at the address[/red] [green]{process_manager_address}[/]"
            )
            unified_shell_log.error(f"Reason: {e}")

            if type(e) == ServerUnreachable:
                unified_shell_log.error(
                    "This can happen if you have the webproxy enabled at CERN"
                )

            if internal_pm and not ctx.obj.pm_process.is_alive():
                unified_shell_log.error(
                    f"[red]The process_manager is dead[/red], exit code {ctx.obj.pm_process.exitcode}"
                )

            if ctx.obj.pm_process.is_alive():
                ctx.obj.pm_process.terminate()
                ctx.obj.pm_process.join()

            sys.exit(1)

    except Exception as e:
        unified_shell_log.error(
            f"[red]Could not connect to the process manager at the address[/red] [green]{process_manager_address}[/]"
        )
        unified_shell_log.error(f"Reason: {e}")

        if type(e) == ServerUnreachable:
            unified_shell_log.error(
                "This can happen if you have the webproxy enabled at CERN"
            )

        if internal_pm and not ctx.obj.pm_process.is_alive():
            unified_shell_log.error(
                f"[red]The process_manager is dead[/red], exit code {ctx.obj.pm_process.exitcode}"
            )

        if ctx.obj.pm_process.is_alive():
            ctx.obj.pm_process.terminate()
            ctx.obj.pm_process.join()

        sys.exit(1)

    if desc.HasField("broadcast"):
        unified_shell_log.debug("Broadcasting")
        ctx.obj.start_listening_pm(
            broadcaster_conf=desc.broadcast,
        )

    unified_shell_log.debug(
        "Adding [green]unified_shell[/green] commands to the context"
    )
    ctx.command.add_command(boot, "boot")
    ctx.obj.dynamic_commands.add("boot")

    unified_shell_log.debug(
        "Adding [green]process_manager[/green] commands to the context"
    )
    ctx.command.add_command(kill, "kill")
    ctx.command.add_command(terminate, "terminate")
    ctx.command.add_command(flush, "flush")
    ctx.command.add_command(logs, "logs")
    ctx.command.add_command(restart, "restart")
    ctx.command.add_command(ps, "ps")
    ctx.obj.dynamic_commands.add("kill")
    ctx.obj.dynamic_commands.add("terminate")
    ctx.obj.dynamic_commands.add("flush")
    ctx.obj.dynamic_commands.add("logs")
    ctx.obj.dynamic_commands.add("restart")
    ctx.obj.dynamic_commands.add("ps")

    # Not particularly proud of this...
    # We instantiate a stateful node which has the same configuration as the one from this session
    # Let's do this
    unified_shell_log.debug("Retrieving the session database")
    db = conffwk.Configuration(ctx.obj.configuration_file)
    session_dal = db.get_dal(class_name="Session", uid=ctx.obj.configuration_id)

    controller_name = session_dal.segment.controller.id
    unified_shell_log.debug("Initializing the [green]ControllerConfHandler[/green]")
    controller_configuration = ControllerConfHandler(
        type=ConfTypes.OKSFileName,
        data=ctx.obj.configuration_file,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=controller_name,
            # some of the function for enable/disable require the full dal of the session
            session=ctx.obj.configuration_id,
        ),
        session_name=session_name,
    )
    os.environ["DUNEDAQ_ELISA_LOGBOOK_APPARATUS"] = "unified_shell"
    fsm_logger = get_logger("controller.FSM")
    fsm_logger.setLevel("ERROR")
    fsm_conf_logger = get_logger("controller.FSMConfHandler")
    fsm_conf_logger.setLevel("ERROR")

    unified_shell_log.debug("Initializing the [green]FSM[/green]")
    fsmch = FSMConfHandler(
        data=controller_configuration.data.controller.fsm,
    )

    unified_shell_log.debug("Initializing the [green]StatefulNode[/green]")
    stateful_node = StatefulNode(fsm_configuration=fsmch, top_segment_controller=False)

    unified_shell_log.debug(
        "Retrieving the transitions from the [green]StatefulNode[/green]"
    )
    transitions = convert_fsm_transition(stateful_node.get_all_fsm_transitions())
    fsm_logger.setLevel(log_level)
    fsm_conf_logger.setLevel(log_level)
    # End of shameful code

    unified_shell_log.debug(
        "Adding [green]controller[/green] commands to the click context"
    )
    for transition in transitions.commands:
        ctx.command.add_command(
            *generate_fsm_command(ctx.obj, transition, controller_name)
        )
        ctx.obj.dynamic_commands.add(format_name_for_cli(transition.name))

    for sequence in session_dal.segment.controller.fsm.command_sequences:
        ctx.command.add_command(
            *generate_fsm_sequence_command(ctx, sequence, controller_name)
        )
        ctx.obj.dynamic_commands.add(format_name_for_cli(sequence.id))

    # Add the controller Click commands to the CLI
    controller_commands = [
        status,
        recompute_status,
        connect,
        disconnect,
        take_control,
        surrender_control,
        who_am_i,
        who_is_in_charge,
        include,
        exclude,
        wait,
        expert_command,
        to_error,
    ]
    for cmd in controller_commands:
        ctx.command.add_command(cmd, format_name_for_cli(cmd.name))
        ctx.obj.dynamic_commands.add(format_name_for_cli(cmd.name))

    # If any of the commands is in the click commands, set batch mode
    if any([arg in ctx.obj.dynamic_commands for arg in sys.argv]):
        ctx.obj.batch_mode = True

    def cleanup():
        """
        Cleanup function to be called on exit.

        This function handles the termination of processes, retraction from the
        connectivity service, and logging shutdown.
        """
        unified_shell_log.info("[green]Exiting unified_shell[/green]")

        if ctx.obj.get_driver("controller", quiet_fail=True):
            try:
                if ctx.obj.get_driver("controller").status().data.in_error:
                    unified_shell_log.warning(
                        "Controller is in error, cannot gracefully shutdown"
                    )
                else:
                    unified_shell_log.info(
                        "Attempting graceful shutdown of the controller"
                    )
                    try:
                        if safe_mode:
                            stop_run_cmd = ctx.command.commands.get("stop-run")
                            scrap_cmd = ctx.command.commands.get("scrap")
                            if stop_run_cmd is not None:
                                ctx.invoke(stop_run_cmd)
                            else:
                                unified_shell_log.warning(
                                    "Command 'stop-run' not found; skipping graceful shutdown step."
                                )
                            if scrap_cmd is not None:
                                ctx.invoke(scrap_cmd)
                            else:
                                unified_shell_log.warning(
                                    "Command 'scrap' not found; skipping graceful shutdown step."
                                )
                            unified_shell_log.info("Controller shutdown gracefully")
                    except Exception as e:
                        unified_shell_log.error(
                            f"Could not shutdown the controller gracefully, reason: {e}"
                        )
            except Exception as e:
                unified_shell_log.error(
                    f"Could not retrieve the controller status, reason: {e}"
                )
            ctx.obj.delete_driver("controller")

        # Retract from the connectivity service
        # Note - this must happen prior to terminating the processes - using a local
        # connectivity service, terminate will kill the connectivity service and retract
        # will log warnings
        unified_shell_log.info(
            f"Retracting session {ctx.obj.session_name} from the connectivity service"
        )
        csc = ConnectivityServiceClient(
            ctx.obj.session_name, connectivity_service_address
        )
        csc.retract_partition(fail_quickly=True)

        # Terminate any residual processes
        ctx.obj.get_driver("process_manager").terminate()

        # Check if any processes are still running
        if (
            len(
                ctx.obj.get_driver("process_manager")
                .ps(ProcessQuery(user=getpass.getuser(), session=ctx.obj.session_name))
                .values
            )
            > 0
        ):
            unified_shell_log.error(
                "Some processes are still running, you might want to check them"
            )

        # Retract the session from the connectivity service if using a standalone
        # (microservice) connectivity service instead of a local process.
        # Needed as daq_applications continue to publish during terminate. With a large
        # number of daq_applications, sending terminate will take some time to get to
        # all the processes. During this time the non-terminated processes will continue
        # to publish to the connectivity service, and this stale information needs to be
        # cleaned up by retracting the session (again).
        if session_dal.connectivity_service.host != "localhost":
            try:
                csc.retract_partition(fail_quickly=True, fail_quietly=True)
                unified_shell_log.info(
                    "Session retracted from the connectivity service"
                )
            except Exception as e:
                unified_shell_log.error(
                    f"Could not retract the session from the connectivity service, reason: {e}"
                )

        # Remove the process manager
        if internal_pm:
            ctx.obj.pm_process.terminate()  # Send a SIGTERM
            ctx.obj.pm_process.join(timeout=2)
            if ctx.obj.pm_process.is_alive():
                unified_shell_log.warning(
                    "Process manager did not exit in time, terminating forcefully."
                )
                ctx.obj.pm_process.kill()  # Send a SIGKILL
                ctx.obj.pm_process.join()
            unified_shell_log.debug("Process manager terminated")

        unified_shell_log.info("[green]unified_shell[/green] exited successfully.")
        logging.shutdown()
        ctx.obj.terminate()
        # os._exit() # Not graceful, can come back in the future for debugging

    ctx.call_on_close(cleanup)

    unified_shell_log.info(
        "[green]unified_shell[/green] ready with [green]process_manager[/green] and [green]controller[/green] commands"
    )
