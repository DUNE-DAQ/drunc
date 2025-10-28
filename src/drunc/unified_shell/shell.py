import getpass
import logging
import multiprocessing as mp
import os
import signal
import sys
import types
from time import sleep
from urllib.parse import ParseResult, urlparse

import click
import click_shell
import conffwk
from druncschema.description_pb2 import Description
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
from drunc.unified_shell.commands import boot, start_shell
from drunc.unified_shell.context import UnifiedShellMode
from drunc.unified_shell.shell_utils import generate_fsm_sequence_command
from drunc.utils.configuration import ConfTypes, OKSKey
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    create_logger_handler,
    format_name_for_cli,
    get_logger,
    ignore_sigint_sighandler,
    log_levels,
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
    """
    The unified shell is a command line interface to interact with the process manager
    and the controller. It can start a process manager if a configuration file is
    provided, or connect to an existing one if a gRPC address is provided. It also
    starts a controller interface to interact with the controller of the DAQ system.

    Args:
        ctx: Click context object.
        process_manager: The process manager configuration file or gRPC address.
        configuration_file: The configuration file to use.
        configuration_id: Configuration ID to use, as defined in the configuration file.
        session_name: The session name to use.
        log_level: The log level to use.
        override_logs: Whether to override logs or not.
        log_path: The log path to use.
        safe_mode: Whether to enable safe mode or not.

    Returns:
        None

    Raises:
        DruncSetupException: If the process manager does not start in time.
        ServerUnreachable: If the process manager is not reachable.
        SystemExit: If the process manager configuration is invalid or if the
            connection to the process manager fails.
    """
    # Set up the drunc and unified_shell loggers
    setup_root_logger(log_level)
    unified_shell_log = get_logger("unified_shell")
    create_logger_handler(rich_handler=True)

    unified_shell_log.debug("Setting up the [green]unified_shell[/green] logger")

    # Parse the process manager argument to determine if it's a config or an address
    process_manager_url: ParseResult = urlparse(process_manager)
    internal_pm: bool = True
    if process_manager_url.scheme == "grpc":  # i.e. if it's an address
        internal_pm = False

    # If using a k8s process manager, validate the session name before proceeding
    if get_pm_type_from_name(
        process_manager
    ) == ProcessManagerTypes.K8s and not validate_k8s_session_name(session_name):
        unified_shell_log.error(
            f"[red]Invalid session/namespace name [bold]({session_name})[/bold][/red]. "
            "Must match RFC1123 label: lowercase alphanumeric or '-', start/end with "
            "alphanumeric, max 63 chars."
        )
        sys.exit(1)

    # Setup configuration related context variables
    ctx.obj.configuration_file = f"oksconflibs:{configuration_file}"
    ctx.obj.configuration_id = configuration_id
    ctx.obj.session_name = session_name

    # Get the session DAL
    db = conffwk.Configuration(ctx.obj.configuration_file)
    session_dal = db.get_dal(class_name="Session", uid=ctx.obj.configuration_id)
    app_log_path = session_dal.log_path

    unified_shell_log.info(
        f"[green]Setting up to use the process manager[/green] with configuration "
        f"[green]{process_manager}[/green] and configuration id [green]"
        f'"{configuration_id}"[/green] from [green]{ctx.obj.configuration_file}[/green]'
    )

    # Establish communication with the process manager, spawning it if needed
    if internal_pm:  # Spawn the Process Manager
        unified_shell_log.debug(
            f"Spawning process_manager with configuration {process_manager}"
        )
        # Check if process_manager is a packaged config
        process_manager_conf_file = get_process_manager_configuration(process_manager)

        # Validate the process manager configuration before starting it
        if not validate_pm_config(process_manager_conf_file):
            unified_shell_log.error(
                "Process manager configuration validation failed. Exiting."
            )
            sys.exit(1)

        # Start the process manager as a separate process
        unified_shell_log.info("Starting process manager")
        ready_event = mp.Event()
        port = mp.Value("i", 0)

        unified_shell_log.debug("[green]Process manager[/green] starting")
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
        unified_shell_log.debug("[green]Process manager[/green] started")

        # Check if the process manager started correctly
        for _ in range(100):
            if ready_event.is_set():
                break
            sleep(0.1)
        if not ready_event.is_set():
            raise DruncSetupException("[red]Process manager didn't start in time[/red]")

        # Setup the process manager address
        process_manager_address = resolve_localhost_and_127_ip_to_network_ip(
            f"localhost:{port.value}"
        )

    else:  # Connect to an existing process manager at the provided address
        process_manager_address = process_manager.replace(
            "grpc://", ""
        )  # remove the grpc scheme
        unified_shell_log.info(
            f"[green]unified_shell[/green] connected to the [green]process_manager"
            f"[/green] at address [green]{process_manager_address}[/green]"
        )

    unified_shell_log.debug(
        f"[green]process_manager[/green] started, communicating through address [green]"
        f"{process_manager_address}[/green]"
    )
    ctx.obj.reset(address_pm=process_manager_address)

    # Run a simple command (describe) to check the connection with the process manager
    desc: Description | None = None
    try:
        desc = ctx.obj.get_driver().describe()
    except Exception as e:
        unified_shell_log.error(
            f"[red]Could not connect to the process manager at the address[/red]"
            f"[green]{process_manager_address}[/green]"
        )
        unified_shell_log.error(f"Reason: {e}")

        if type(e) == ServerUnreachable:
            unified_shell_log.error(
                "This can happen if you have the webproxy enabled at CERN"
            )

        if internal_pm and not ctx.obj.pm_process.is_alive():
            unified_shell_log.error(
                f"[red]The process_manager is dead[/red], exit code "
                f"{ctx.obj.pm_process.exitcode}"
            )

        if ctx.obj.pm_process.is_alive():
            ctx.obj.pm_process.terminate()
            ctx.obj.pm_process.join()

        sys.exit(1)

    # Broadcasting configuration if requested
    if desc.HasField("broadcast"):
        unified_shell_log.debug("Broadcasting")
        ctx.obj.start_listening_pm(
            broadcaster_conf=desc.broadcast,
        )

    # Add the unified shell Click commands to the CLI
    unified_shell_log.debug("Adding [green]unified_shell[/green] commands")
    ctx.command.add_command(boot, "boot")
    ctx.obj.dynamic_commands.add("boot")

    # Add the process manager Click commands to the CLI
    unified_shell_log.debug("Adding [green]process_manager[/green] commands")
    process_manager_commands: list[click.Command] = [
        kill,
        terminate,
        flush,
        logs,
        restart,
        ps,
    ]
    for cmd in process_manager_commands:
        ctx.command.add_command(cmd, format_name_for_cli(cmd.name))
        ctx.obj.dynamic_commands.add(format_name_for_cli(cmd.name))

    # Get all the controller commands by instantiating the stateful node defined in the
    # configuration and getting the FSM transitions from it.
    unified_shell_log.debug("Defining the pseudo controller to get its FSM commands")
    controller_name = session_dal.segment.controller.id
    controller_configuration = ControllerConfHandler(
        type=ConfTypes.OKSFileName,
        data=ctx.obj.configuration_file,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=controller_name,
            session=ctx.obj.configuration_id,
        ),
        session_name=ctx.obj.session_name,
    )
    # Avoid setting up the ELISA logbook for the unified shell
    os.environ["DUNEDAQ_ELISA_LOGBOOK_APPARATUS"] = "unified_shell"

    unified_shell_log.debug("Initializing the [green]FSM[/green]")
    fsmch = FSMConfHandler(data=controller_configuration.data.controller.fsm)

    unified_shell_log.debug("Initializing the [green]StatefulNode[/green]")
    stateful_node = StatefulNode(fsm_configuration=fsmch, top_segment_controller=False)

    unified_shell_log.debug("Retrieving the [green]StatefulNode[/green] transitions")
    transitions = convert_fsm_transition(stateful_node.get_all_fsm_transitions())

    # Add the FSM transitions and sequences as Click commands to the CLI
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
    controller_commands: list[click.Command] = [
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
        ctx.obj.running_mode = UnifiedShellMode.BATCH
        ctx.command.add_command(start_shell, "start-shell")
        ctx.obj.dynamic_commands.add("start-shell")

    # If start-shell is in the arguments, set semibatch mode
    if "start-shell" in sys.argv:
        ctx.obj.running_mode = UnifiedShellMode.SEMIBATCH

    def cleanup():
        """
        Cleanup function to be called on exit.

        This function handles the termination of processes, retraction from the
        connectivity service, and logging shutdown.
        """
        unified_shell_log.info("[green]Shutting down the unified_shell[/green]")

        # Attempt a stateful shutdown of the controller if possible, returning to
        # initial state before terminating
        if ctx.obj.get_driver("controller", quiet_fail=True):
            try:
                if ctx.obj.get_driver("controller").status().status.in_error:
                    unified_shell_log.warning(
                        "Controller is in error, cannot gracefully shutdown"
                    )
                else:
                    # Safe mode crucial for use with hardware
                    if safe_mode:
                        try:
                            unified_shell_log.info(
                                "Attempting graceful shutdown of the controller"
                            )
                            stop_run_cmd = ctx.command.commands.get("stop-run")
                            scrap_cmd = ctx.command.commands.get("scrap")
                            if stop_run_cmd is not None:
                                ctx.invoke(stop_run_cmd)
                            else:
                                unified_shell_log.warning(
                                    "Command 'stop-run' not found; skipping graceful "
                                    "shutdown step."
                                )
                            if scrap_cmd is not None:
                                ctx.invoke(scrap_cmd)
                            else:
                                unified_shell_log.warning(
                                    "Command 'scrap' not found; skipping graceful "
                                    "shutdown step."
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

        # Terminate any residual processes
        if ctx.obj.get_driver("process_manager"):
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
            connectivity_service_address: str = (
                f"{session_dal.connectivity_service.host}:"
                f"{session_dal.connectivity_service.service.port}"
            )
            csc = ConnectivityServiceClient(
                ctx.obj.session_name, connectivity_service_address
            )
            try:
                csc.retract_partition(fail_quickly=True, fail_quietly=True)
                unified_shell_log.debug(
                    "Session retracted from the connectivity service"
                )
            except Exception as e:
                unified_shell_log.error(
                    f"Could not retract the session from the connectivity service: {e}"
                )

        # Remove the process manager
        if internal_pm:
            ctx.obj.pm_process.terminate()  # Send a SIGTERM to the pm_process
            ctx.obj.pm_process.join(timeout=2)  # Block continuing execution for 2s
            if ctx.obj.pm_process.is_alive():
                unified_shell_log.warning(
                    "Process manager did not exit in time, terminating forcefully."
                )
                ctx.obj.pm_process.kill()  # Send a SIGKILL
                ctx.obj.pm_process.join()  # Block until the process is dead
            unified_shell_log.debug("Process manager terminated")

        unified_shell_log.info("[green]unified_shell exited successfully[/green]")
        logging.shutdown()  # Shutdown logging
        ctx.obj.terminate()  # Terminate the broadcasters in the context
        ctx.exit()  # Close the click context

    ctx.call_on_close(cleanup)

    # Handle SIGTERM to gracefully shutdown the unified_shell
    def signal_sigterm_handler(signum: int, frame: types.FrameType) -> None:
        """
        Handle the SIGTERM signal to gracefully shut down the unified_shell.

        Args:
            signum: The signal number.
            frame: The current stack frame (not used).
        """
        unified_shell_log.info(
            "[red]SIGTERM received, shutting down unified_shell...[/red]"
        )
        ctx.exit()
        return

    # Register the SIGTERM handler to gracefully shut down the server
    signal.signal(signal.SIGTERM, signal_sigterm_handler)

    unified_shell_log.info(
        "[green]unified_shell[/green] ready with [green]process_manager[/green] and [green]controller[/green] commands"
    )


@unified_shell.result_callback()
@click.pass_context
def _maybe_enter_shell(ctx, results, **_):
    # If user requested interactive mode at the end
    if ctx.obj.running_mode == UnifiedShellMode.SEMIBATCH:
        sh = click_shell.make_click_shell(ctx, prompt=ctx.command.shell.prompt)
        sh.cmdloop()
