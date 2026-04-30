import functools

import click
import conffwk
from druncschema.controller_pb2 import DescribeFSMResponse
from druncschema.process_manager_pb2 import ProcessInstanceList, ProcessQuery

from drunc.controller.controller_driver import ControllerDriver
from drunc.exceptions import DruncException, DruncSetupException
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.utils import format_name_for_cli, get_logger


def run_fsm_sequence(
    sequence_commands: list[str],
    sequence_command_opts_and_args: dict[str, list[str]],
    ctx: click.core.Context,
    obj: click.core.Context,
    **kwargs,
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

    # Check all required parameters for all commands in the sequence before executing
    # any command
    for cmd_name in sequence_commands:
        # Get the sub-command to check its parameters
        sub_cmd: click.core.Command = ctx.command.commands[cmd_name]

        # Check if all required parameters for the sub-command are provided in kwargs
        # If any required parameter is missing, log an error and exit
        for param in sub_cmd.get_params(ctx):  # type: click.core.Option
            # If the parameter is required and not provided, log an error and return
            if param.required and kwargs.get(param.name) is None:
                flag_display = param.opts[0] if param.opts else param.name
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
        pmd: ProcessManagerDriver | None = obj.get_driver(
            "process_manager", quiet_fail=True
        )
        if command == "boot":
            process_list: ProcessInstanceList = pmd.ps(ProcessQuery(names=[".*"]))
            if not process_list.values:  # We haven't started anything yet
                accepted_command.append("boot")
        elif command == "terminate":
            process_list: ProcessInstanceList = pmd.ps(ProcessQuery(names=[".*"]))
            if (
                process_list.values
            ):  # We have started something that needs to be terminated
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
        sub_cmd: click.core.Command = ctx.command.commands[command]

        # Build command kwargs
        cmd_kwargs: dict(str, bool | str | int | float | None) = {
            param.name: kwargs[param.name]
            for param in sub_cmd.get_params(ctx)
            if param.name in kwargs
        }

        # Invoke the command with the appropriate kwargs
        try:
            logger.info(f"Running command: '{command}'")
            ctx.invoke(sub_cmd, **cmd_kwargs)
        except DruncException as e:
            logger.error(f"Error running command: '{command}'")
            raise e


def generate_fsm_sequence_command(
    ctx: click.core.Context, sequence: "conffwk.dal.FSMsequence", controller_name: str
):
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

    sequence_options: dict[str, conffwk.dal.FSMParameter] = {}  # {option_name: option}

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
        if command_name not in ctx.command.commands.keys():
            raise DruncSetupException(
                f"Command {command_name} required by sequence {sequence.id} not found in the command list!"
            )
        sequence_commands.append(command_name)

        # Extend the help string
        sequence_str += f"{command_name} {middle_text}"

        # Gather the command parameters, add them to the command options and args
        params: list(click.core.Option) = ctx.command.commands[command_name].get_params(
            ctx
        )
        sequence_command_options[command_name] = []
        for param in params:  #  type: click.core.Option
            if param.name == "help":
                continue
            sequence_command_options[command_name].append(param.name)
            sequence_options[param.name] = param

    # Construct the sequence function
    cmd: functools.partial = functools.partial(
        run_fsm_sequence, sequence_commands, sequence_command_options, ctx
    )
    cmd = click.pass_obj(cmd)

    # Add click options to the function
    for param_name, param in sequence_options.items():  # type: str, conffwk.dal.FSMParameter
        if param.name == "help":
            continue

        param_name: str = format_name_for_cli(param_name)
        param_default: str | int | float | bool | None = (
            param.default if param.default is not None else None
        )
        cmd = click.option(
            f"--{param_name}",
            type=param.type,
            default=param_default,
            show_default=param.show_default,
            required=param.required,
            help=param.help,
        )(cmd)

    # Transform the function into a Click command
    cmd: click.core.Command = click.command(
        name=format_name_for_cli(sequence.id),
        help=f"Run the sequence {sequence.id}: {sequence_str}",
    )(cmd)

    return cmd, format_name_for_cli(sequence.id)


def resource_log_tree(data, log, prefix=""):
    items = list(data.items())
    total = len(items)

    for i, (key, value) in enumerate(items):
        is_last = i == total - 1
        connector = "└── " if is_last else "├── "

        # Check if the value is a nested segment (resources stored as dict)
        if isinstance(value, dict) and value:
            extension = "    " if is_last else "│   "
            resource_log_tree(value, prefix + extension)

        else:
            display_val = str(value)
            log.info(f"{prefix}{connector}{key}: {display_val}")
