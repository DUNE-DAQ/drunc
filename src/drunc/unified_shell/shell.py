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
from daqpytools.logging import logging_log_levels
from druncschema.process_manager_pb2 import ProcessQuery

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.controller.configuration import ControllerConfHandler
from drunc.controller.interface.commands import (
    connect,
    disconnect,
    echo,
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
from drunc.exceptions import (
    DruncBatchShellArgError,
    DruncBatchShellError,
    DruncBatchShellMissingArg,
    DruncBatchShellUnknownCommand,
    DruncSetupException,
)
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
    format_name_for_cli,
    get_logger,
    get_root_logger,
    ignore_sigint_sighandler,
    resolve_localhost_and_127_ip_to_network_ip,
)


@click_shell.shell(
    prompt="drunc-unified-shell > ",
    chain=True,
    hist_file=os.path.expanduser("~") + "/.drunc-unified-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
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
    ctx: click.core.Context,
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
    get_root_logger(log_level)
    ctx.obj.log = get_logger("unified_shell", rich_handler=True)
    ctx.obj.log.debug("Setting up the [green]unified_shell[/green] logger")

    # Parse the process manager argument to determine if it's a config or an address
    process_manager_url: ParseResult = urlparse(process_manager)
    internal_pm: bool = True
    if process_manager_url.scheme == "grpc":  # i.e. if it's an address
        internal_pm = False

    # If using a k8s process manager, validate the session name before proceeding
    if get_pm_type_from_name(
        process_manager
    ) == ProcessManagerTypes.K8s and not validate_k8s_session_name(session_name):
        ctx.obj.log.error(
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

    ctx.obj.log.info(
        f"[green]Setting up to use the process manager[/green] with configuration "
        f"[green]{process_manager}[/green] and configuration id [green]"
        f'"{configuration_id}"[/green] from [green]{ctx.obj.configuration_file}[/green]'
    )

    # Establish communication with the process manager, spawning it if needed
    if internal_pm:  # Spawn the Process Manager
        ctx.obj.log.debug(
            f"Spawning process_manager with configuration {process_manager}"
        )
        # Check if process_manager is a packaged config
        process_manager_conf_file = get_process_manager_configuration(process_manager)

        # Validate the process manager configuration before starting it
        if not validate_pm_config(process_manager_conf_file):
            ctx.obj.log.error(
                "Process manager configuration validation failed. Exiting."
            )
            sys.exit(1)

        # Start the process manager as a separate process
        ctx.obj.log.info("Starting process manager")
        ready_event = mp.Event()
        port = mp.Value("i", 0)

        ctx.obj.log.debug("[green]Process manager[/green] starting")
        ctx.obj.override_logs = override_logs
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
        ctx.obj.log.debug("[green]Process manager[/green] started")

        # Check if the process manager started correctly
        process_started = False
        for _ in range(100):  # 10s timeout
            if ready_event.is_set():
                process_started = True
                break

            if not ctx.obj.pm_process.is_alive():
                exit_code = ctx.obj.pm_process.exitcode
                ctx.obj.log.error(
                    f"[red]Process manager process died unexpectedly with exit code {exit_code}."
                )
                ctx.obj.log.error(
                    "[red]This is likely a configuration error (e.g., bad kube-config)."
                )
                ctx.obj.log.error(
                    "[red]Please check the full traceback in the terminal above this message.[/red]"
                )
                sys.exit(exit_code if exit_code else 1)
            sleep(0.1)

        if not process_started:
            # This message will only show if the process is *alive* but never sent the "ready" signal
            raise DruncSetupException(
                "[red]Process manager timed out starting. Check logs for details.[/red]"
            )

        # Setup the process manager address
        process_manager_address = resolve_localhost_and_127_ip_to_network_ip(
            f"localhost:{port.value}"
        )

    else:  # Connect to an existing process manager at the provided address
        process_manager_address = process_manager.replace(
            "grpc://", ""
        )  # remove the grpc scheme
        ctx.obj.log.info(
            f"[green]unified_shell[/green] connected to the [green]process_manager"
            f"[/green] at address [green]{process_manager_address}[/green]"
        )

    ctx.obj.log.debug(
        f"[green]process_manager[/green] started, communicating through address [green]"
        f"{process_manager_address}[/green]"
    )
    ctx.obj.reset(address_pm=process_manager_address)

    # Run a simple command (describe) to check the connection with the process manager
    try:
        ctx.obj.get_driver().describe()
    except Exception as e:
        ctx.obj.log.error(
            f"[red]Could not connect to the process manager at the address: [/red]"
            f"[green]{process_manager_address}[/green]"
        )
        ctx.obj.log.debug(f"Reason: {e}")

        if type(e) == ServerUnreachable:
            ctx.obj.log.error(
                "[red]This can happen if you have the webproxy enabled at CERN. Ensure "
                "http_proxy, https_proxy, no_proxy, and equivalent aren't set. [/red]"
            )

        if internal_pm and not ctx.obj.pm_process.is_alive():
            ctx.obj.log.error(
                f"[red]The process_manager is dead[/red], exit code "
                f"{ctx.obj.pm_process.exitcode}"
            )

        if ctx.obj.pm_process.is_alive():
            ctx.obj.pm_process.terminate()
            ctx.obj.pm_process.join()

        sys.exit(1)

    # Add the unified shell Click commands to the CLI
    ctx.obj.log.debug("Adding [green]unified_shell[/green] commands")
    ctx.command.add_command(boot, "boot")
    ctx.obj.dynamic_commands.add("boot")

    # Add the process manager Click commands to the CLI
    ctx.obj.log.debug("Adding [green]process_manager[/green] commands")
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
    ctx.obj.log.debug("Defining the pseudo controller to get its FSM commands")
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

    ctx.obj.log.debug("Initializing the [green]FSM[/green]")

    #! This is an absolute _hack_ because we do not want the
    # unified shell fsm to go to tty but want other fsms to do so
    # live with it. At least until controller.core uses file handler instead of stream
    get_logger("controller.core.FSM", log_level="CRITICAL")

    fsmch = FSMConfHandler(data=controller_configuration.data.controller.fsm)

    ctx.obj.log.debug("Initializing the [green]StatefulNode[/green]")
    stateful_node = StatefulNode(fsm_configuration=fsmch, top_segment_controller=False)

    ctx.obj.log.debug("Retrieving the [green]StatefulNode[/green] transitions")
    transitions = convert_fsm_transition(stateful_node.get_all_fsm_transitions())

    # Add the FSM transitions and sequences as Click commands to the CLI
    ctx.obj.log.debug("Adding [green]controller[/green] commands to the click context")
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
        echo,
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

    parser = ctx.command.make_parser(ctx)
    _, extract_batch_args, _ = parser.parse_args(sys.argv[1:])
    ctx.obj.batch_commands = extract_batch_args

    # If any of the commands is in the click commands, set batch mode
    if ctx.obj.batch_commands:
        ctx.obj.running_mode = UnifiedShellMode.BATCH
        ctx.command.add_command(start_shell, "start-shell")
        ctx.obj.dynamic_commands.add("start-shell")
        validate_chain(ctx, extract_batch_args)

    # If start-shell is in the arguments, set semibatch mode
    if "start-shell" in ctx.obj.batch_commands:
        ctx.obj.running_mode = UnifiedShellMode.SEMIBATCH

    def cleanup():
        """
        Cleanup function to be called on exit.

        This function handles the termination of processes, retraction from the
        connectivity service, and logging shutdown.
        """
        ctx.obj.log.info("[green]Shutting down the unified_shell[/green]")

        # Attempt a stateful shutdown of the controller if possible, returning to
        # initial state before terminating
        if ctx.obj.get_driver("controller", quiet_fail=True):
            try:
                if ctx.obj.get_driver("controller").status().status.in_error:
                    ctx.obj.log.warning(
                        "Controller is in error, cannot gracefully shutdown"
                    )
                else:
                    # Safe mode crucial for use with hardware
                    if safe_mode:
                        try:
                            ctx.obj.log.info(
                                "Attempting graceful shutdown of the controller"
                            )
                            stop_run_cmd = ctx.command.commands.get("stop-run")
                            scrap_cmd = ctx.command.commands.get("scrap")
                            if stop_run_cmd is not None:
                                ctx.invoke(stop_run_cmd)
                            else:
                                ctx.obj.log.warning(
                                    "Command 'stop-run' not found; skipping graceful "
                                    "shutdown step."
                                )
                            if scrap_cmd is not None:
                                ctx.invoke(scrap_cmd)
                            else:
                                ctx.obj.log.warning(
                                    "Command 'scrap' not found; skipping graceful "
                                    "shutdown step."
                                )
                            ctx.obj.log.info("Controller shutdown gracefully")
                        except Exception as e:
                            ctx.obj.log.error(
                                f"Could not shutdown the controller gracefully, reason: {e}"
                            )
            except Exception as e:
                ctx.obj.log.error(
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
            ctx.obj.log.error(
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
                ctx.obj.log.debug("Session retracted from the connectivity service")
            except Exception as e:
                ctx.obj.log.error(
                    f"Could not retract the session from the connectivity service: {e}"
                )

        # Remove the connection to the process manager
        ctx.obj.get_driver("process_manager").close()
        ctx.obj.delete_driver("process_manager")

        # Remove the process manager
        if internal_pm:
            ctx.obj.pm_process.terminate()  # Send a SIGTERM to the pm_process
            ctx.obj.pm_process.join(timeout=2)  # Block continuing execution for 2s
            if ctx.obj.pm_process.is_alive():
                ctx.obj.log.warning(
                    "Process manager did not exit in time, terminating forcefully."
                )
                ctx.obj.pm_process.kill()  # Send a SIGKILL
                ctx.obj.pm_process.join()  # Block until the process is dead
            ctx.obj.log.debug("Process manager terminated")

        ctx.obj.log.info("[green]unified_shell exited successfully[/green]")
        logging.shutdown()
        ctx.obj.terminate()
        ctx.exit()

    ctx.call_on_close(cleanup)

    # Handle SIGTERM to gracefully shutdown the unified_shell
    def signal_sigterm_handler(signum: int, frame: types.FrameType) -> None:
        """
        Handle the SIGTERM signal to gracefully shut down the unified_shell.

        Args:
            signum: The signal number.
            frame: The current stack frame (not used).
        """
        ctx.obj.log.info("[red]SIGTERM received, shutting down unified_shell...[/red]")
        ctx.exit()
        return

    # Register the SIGTERM handler to gracefully shut down the server
    signal.signal(signal.SIGTERM, signal_sigterm_handler)

    ctx.obj.log.info(
        "[green]unified_shell[/green] ready with [green]process_manager[/green] and [green]controller[/green] commands"
    )


@unified_shell.result_callback()
@click.pass_context
def _maybe_enter_shell(ctx, results, **_):
    # If user requested interactive mode at the end
    if ctx.obj.running_mode == UnifiedShellMode.SEMIBATCH:
        sh = click_shell.make_click_shell(ctx, prompt=ctx.command.shell.prompt)
        sh.cmdloop()


def split_chain(
    chain_args: list[str], command_names: set[str]
) -> list[tuple[str, list[str]]]:
    """Partition command-line arguments into individual command invocations.
    Args:
        chain_args (list): Flattened list of all command tokens and arguments.
        command_names (set): Set of valid command names to use as boundaries.

    Returns:
        list[tuple]: List of (command_name, command_args) tuples representing
            individual command invocations in sequence order.

    Raises:
        click.UsageError: If a token in chain_args doesn't match any known
            command name when expected to be a command.
    """
    parts = []
    i = 0
    while i < len(chain_args):
        cmd = chain_args[i]
        if cmd not in command_names:
            raise DruncBatchShellUnknownCommand(cmd)
        i += 1
        cmd_args = []
        while i < len(chain_args) and chain_args[i] not in command_names:
            cmd_args.append(chain_args[i])
            i += 1
        parts.append((cmd, cmd_args))
    return parts


def validate_chain(ctx: click.core.Context, chain_args: list[str]) -> None:
    """Validate syntax and arguments for all commands in a chain.
    Args:
        ctx: Click context object containing the command registry.
        chain_args (list): Flattened list of tokens from extract_chain_tokens.
    """
    command_names = set(ctx.command.commands.keys())

    def command_can_consume_more_positionals(
        command: click.Command, provided_args: list[str]
    ) -> bool:
        argument_params = [p for p in command.params if isinstance(p, click.Argument)]
        if not argument_params:
            return False

        remaining_tokens = len(provided_args)
        for argument_param in argument_params:
            if argument_param.nargs == -1:
                return True

            if remaining_tokens >= argument_param.nargs:
                remaining_tokens -= argument_param.nargs
            else:
                return True

        return False

    try:
        chained_cmds = split_chain(chain_args, command_names)
        for index, (cmd_name_real, cmd_args_real) in enumerate(chained_cmds):
            cmd_name = cmd_name_real
            cmd_args = cmd_args_real.copy()
            cmd_args_static = cmd_args_real.copy()

            sub_cmd: click.Command = ctx.command.commands[cmd_name]

            # Validate the command as split by split_chain
            sub_cmd.make_context(
                cmd_name, cmd_args, parent=ctx, resilient_parsing=False
            )

            # Also validate boundary tokens that could still be consumed as positional
            # arguments by the current command. This catches cases like
            # "wait conf", where "conf" is a command token but Click may attempt to
            # parse it as wait's positional argument first.
            cmd_args = cmd_args_real.copy()
            if (
                index + 1 < len(chained_cmds)
                and not cmd_args
                and command_can_consume_more_positionals(sub_cmd, cmd_args)
            ):
                next_cmd_name, _ = chained_cmds[index + 1]
                prev_cmd_name, _ = chained_cmds[index]

                try:
                    sub_cmd.make_context(
                        cmd_name,
                        [*cmd_args, next_cmd_name],
                        parent=ctx,
                        resilient_parsing=False,
                    )
                except click.ClickException as e:
                    raise DruncBatchShellMissingArg(prev_cmd_name, next_cmd_name) from e

    except click.NoSuchOption as e:
        raise DruncBatchShellError(e) from e

    except click.UsageError as e:
        raise DruncBatchShellArgError(cmd_args_static) from e

    except click.ClickException as e:
        raise DruncBatchShellError(e) from e
