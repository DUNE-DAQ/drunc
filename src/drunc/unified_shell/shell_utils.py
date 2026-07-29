import functools
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, cast

import click
from druncschema.controller_pb2 import DescribeFSMResponse
from druncschema.process_manager_pb2 import ProcessInstanceList, ProcessQuery

from drunc.controller.controller_driver import ControllerDriver
from drunc.exceptions import DruncException, DruncSetupException
from drunc.unified_shell.context import UnifiedShellContext
from drunc.utils.utils import format_name_for_cli, get_logger

if TYPE_CHECKING:
    pass


class SequenceEntryLike(Protocol):
    id: str


class SequenceOptionLike(Protocol):
    name: str | None
    default: str | int | float | bool | None
    show_default: bool
    required: bool
    help: str | None
    type: object


class FSMSequenceLike(Protocol):
    id: str
    sequence: Sequence[SequenceEntryLike]


def run_fsm_sequence(
    sequence_commands: list[str],
    sequence_command_opts_and_args: dict[str, list[str]],
    ctx: click.core.Context,
    obj: UnifiedShellContext,
    **kwargs: str | int | float | bool | None,
) -> None:
    """
    Execute a command sequence by invoking individual commands in order.

    This function takes a list of command names representing a sequence and invokes
    each command in order using the provided Click context. It gathers the necessary
    options and arguments for each command from the provided keyword arguments.

    Note - sequence commands are the names of the commands in the sequence, while the
    sequence name is the name of the overall sequence being executed.

    Args:
        sequence_commands (list[str]): List of command names to execute in order.
        sequence_command_opts_and_args (dict[str, list[str]]): Mapping of sequence command names to their options and arguments.
        ctx (click.core.Context): The Click context for invoking commands.
        obj (click.core.Context): The object passed to commands, typically containing shared state.
        **kwargs: Additional keyword arguments representing command options and arguments.
    """
    logger = get_logger("unified_shell.shell_utils")
    logger.info(f"Running sequence: {sequence_commands}")
    command_group = cast(click.Group, ctx.command)

    # Check all required parameters for all commands in the sequence before executing
    # any command
    for cmd_name in sequence_commands:
        # Get the sub-command to check its parameters
        check_cmd: click.Command = command_group.commands[cmd_name]

        # Check if all required parameters for the sub-command are provided in kwargs
        # If any required parameter is missing, log an error and exit
        for param in check_cmd.get_params(ctx):
            # If the parameter is required and not provided, log an error and return
            if param.name is None:
                continue
            if param.required and kwargs.get(param.name) is None:
                if isinstance(param, click.Option):
                    flag_display = param.opts[0] if param.opts else param.name
                else:
                    flag_display = param.name
                logger.error(
                    f"Aborting sequence! Command '{cmd_name}' requires "
                    f"'{flag_display}' but it was not provided."
                )
                return

    # Iterate through the sequence commands and invoke them with the appropriate options
    # and arguments
    for command in sequence_commands:  # type: str
        # Define the set of commands that can be ran
        accepted_command: list[str] = []

        # These commands are not stateful. If they are a part of the sequence, they
        # should be run regardless of their position in the sequence
        pmd = obj.get_driver("process_manager", quiet_fail=True)
        process_list: ProcessInstanceList | None = None
        if command == "boot":
            if pmd is not None:
                process_list = pmd.ps(ProcessQuery(names=[".*"]))
            if process_list is not None and not process_list.values:
                accepted_command.append("boot")
        elif command == "terminate":
            if pmd is not None:
                process_list = pmd.ps(ProcessQuery(names=[".*"]))
            if process_list is not None and process_list.values:
                accepted_command.append("terminate")

        # Get the FSM commands that can be ran from the current state
        controller_driver: ControllerDriver | None = obj.get_driver(
            "controller", quiet_fail=True
        )
        if controller_driver:
            accepted_command_raw: DescribeFSMResponse = controller_driver.describe_fsm()
            accepted_command += [
                format_name_for_cli(c.name)
                for c in accepted_command_raw.description.commands
            ]

        # If the command is not in the list of accepted commands, skip it and move on to
        # the next command in the sequence
        if command not in accepted_command:
            logger.info(
                f"Command '{command}' cannot be run in the current state, skipping."
            )
            continue

        # Get the sub-command to invoke
        invoke_cmd: click.Command = command_group.commands[command]

        # Build command kwargs
        cmd_kwargs: dict[str, bool | str | int | float | None] = {
            param.name: kwargs[param.name]
            for param in invoke_cmd.get_params(ctx)
            if param.name is not None and param.name in kwargs
        }

        # Invoke the command with the appropriate kwargs
        try:
            logger.info(f"Running command: '{command}'")
            ctx.invoke(invoke_cmd, **cmd_kwargs)
        except DruncException:
            logger.error(f"Error running command: '{command}'")
            raise


def generate_fsm_sequence_command(
    ctx: click.core.Context,
    sequence: FSMSequenceLike,
    controller_name: str,
) -> tuple[click.Command, str]:
    """
    Parse a FSM sequence and generate a Click command to run it.

    This command extracts the OKS FSMsequence object attributes and generates a Click
    command that runs the sequence by invoking the individual commands in order.
    The generated command includes options for all parameters of the individual commands
    in the sequence.

    Note - "sequence" is the name of the FSMsequence, "sequence_command" is the name of
    an individual command in the sequence.

    Args:
        ctx (click.core.Context): The Click context.
        sequence (conffwk.dal.FSMsequence): The FSM sequence object.
        controller_name (str): The name of the controller.

    Returns:
        tuple: The generated Click command and its name.
    """

    # Prepare the command
    sequence_commands: list[str] = []
    sequence_command_options: dict[
        str, list[str]
    ] = {}  # {sequence_command: [sequence_command_option_name]}

    sequence_options: dict[str, SequenceOptionLike] = {}
    command_group = cast(click.Group, ctx.command)

    command_ids: list[str] = [command.id for command in sequence.sequence]

    # Build the command string for help
    sequence_str: str = ""
    middle_text: str = "[optionally], then "

    # Special handling for start_run and shutdown sequences
    if sequence.id == "start_run":
        command_ids = ["boot"] + command_ids
    elif sequence.id == "shutdown":
        command_ids = command_ids + ["terminate"]

    # Parse the sequence commands, construct the command string, and gather parameters
    for command_id in command_ids:  # type: str
        # Parse the sequence command id to match the Click command name
        command_name: str = format_name_for_cli(command_id)
        if command_name not in command_group.commands.keys():
            raise DruncSetupException(
                f"Command {command_name} required by sequence {sequence.id} not found in the command list!"
            )
        sequence_commands.append(command_name)

        # Extend the help string
        sequence_str += f"{command_name} {middle_text}"

        # Gather the command parameters, add them to the command options and args
        params = command_group.commands[command_name].get_params(ctx)
        sequence_command_options[command_name] = []
        for param in params:
            if not isinstance(param, click.Option):
                continue
            if param.name in (None, "help"):
                continue
            sequence_command_options[command_name].append(param.name)
            sequence_options[param.name] = cast(SequenceOptionLike, param)

    # Construct the sequence function
    base_cmd_fn = functools.partial(
        run_fsm_sequence, sequence_commands, sequence_command_options, ctx
    )
    cmd_fn_with_obj = click.pass_obj(base_cmd_fn)

    # Add click options to the function
    for option_name, option in sequence_options.items():
        if option.name == "help":
            continue

        option_name_cli = format_name_for_cli(option_name)
        option_default: str | int | float | bool | None = (
            option.default if option.default is not None else None
        )
        cmd_fn_with_obj = click.option(
            f"--{option_name_cli}",
            type=option.type,
            default=option_default,
            show_default=option.show_default,
            required=option.required,
            help=option.help,
        )(cmd_fn_with_obj)

    # Transform the function into a Click command
    cmd = click.command(
        name=format_name_for_cli(sequence.id),
        help=f"Run the sequence {sequence.id}: {sequence_str}",
    )(cmd_fn_with_obj)

    return cmd, format_name_for_cli(sequence.id)
