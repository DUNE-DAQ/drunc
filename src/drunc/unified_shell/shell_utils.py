from functools import partial

import click
import conffwk
from druncschema.process_manager_pb2 import ProcessQuery

from drunc.exceptions import DruncException, DruncSetupException
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
    for command in sequence_commands:
        accepted_command = ["terminate"]  # Always accept terminate

        controller_driver = obj.get_driver("controller", quiet_fail=True)
        if command == "boot":
            pmd = obj.get_driver("process_manager", quiet_fail=True)
            process_list = pmd.ps(ProcessQuery(names=[".*"]))
            if not process_list.values:  # We haven't started anything yet
                accepted_command.append("boot")
        if controller_driver:
            accepted_command_raw = controller_driver.describe_fsm()
            accepted_command += [
                format_name_for_cli(c.name)
                for c in accepted_command_raw.description.commands
            ]
        logger.debug(f"Accepted commands: {accepted_command}")

        if command not in accepted_command and command != [sequence_commands[-1]]:
            logger.info(f"Skipping command '{command}'")
            continue

        logger.info(f"Running command: '{command}'")

        cmd_kwargs = {
            kwarg_name: kwargs[kwarg_name]
            for kwarg_name in sequence_command_opts_and_args[command]
        }

        try:
            ctx.invoke(ctx.command.commands[command], **cmd_kwargs)
        except DruncException as e:
            logger.error(f"Error running command: '{command}'")
            logger.exception(e)
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
    sequence_options: dict[
        str, conffwk.dal.FSMParameter
    ] = {}  # {option_name: option}, dict removes duplicates
    command_ids = [command.id for command in sequence.sequence]

    # Build the command string for help
    sequence_str = ""
    middle_text = "[optionally], then "

    # Special handling for start_run and shutdown sequences
    if sequence.id == "start_run":
        command_ids = ["boot"] + command_ids
    elif sequence.id == "shutdown":
        command_ids = command_ids + ["terminate"]

    # Parse the sequence commands, construct the command string, and gather parameters
    for command_id in command_ids:
        # Parse the sequence command id to match the Click command name
        command_name = format_name_for_cli(command_id)
        if command_name not in ctx.command.commands.keys():
            raise DruncSetupException(
                f"Command {command_name} required by sequence {sequence.id} not found in the command list!"
            )
        sequence_commands.append(command_name)

        # Extend the help string
        sequence_str += f"{command_name} {middle_text}"

        # Gather the command parameters, add them to the command options and args
        params = ctx.command.commands[command_name].get_params(ctx)
        sequence_command_options[command_name] = []
        for param in params:
            if param.name == "help":
                continue
            sequence_command_options[command_name].append(param.name)
            sequence_options[param.name] = param

    # Construct the sequence function
    cmd = partial(run_fsm_sequence, sequence_commands, sequence_command_options, ctx)
    cmd = click.pass_obj(cmd)

    # Add click options to the function
    for param_name, param in sequence_options.items():
        if param.name == "help":
            continue

        param_name = format_name_for_cli(param_name)
        cmd = click.option(
            f"--{param_name}",
            type=param.type,
            default=param.default,
            show_default=param.show_default,
            required=param.required,
            help=param.help,
        )(cmd)

    # Transform the function into a Click command
    cmd = click.command(
        name=format_name_for_cli(sequence.id),
        help=f"Run the sequence {sequence.id}: {sequence_str}",
    )(cmd)

    return cmd, format_name_for_cli(sequence.id)
