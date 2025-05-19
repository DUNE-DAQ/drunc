import asyncio
from functools import partial

import click
from druncschema.process_manager_pb2 import ProcessQuery

from drunc.exceptions import DruncException, DruncSetupException
from drunc.utils.utils import get_logger

logger = get_logger("unified_shell.shell_utils")


def run_fsm_sequence(sequence_commands, cmd_to_options_and_args, ctx, obj, **kwargs):
    logger.info(f"Running sequence: {sequence_commands}")

    for command in sequence_commands:
        accepted_command = ["terminate"]  # Always accept terminate

        cd = obj.get_driver("controller", quiet_fail=True)
        if command == "boot":
            pmd = obj.get_driver("process_manager", quiet_fail=True)
            loop = asyncio.get_event_loop()
            main_task = asyncio.ensure_future(pmd.ps(ProcessQuery(names=[".*"])))
            process_list = loop.run_until_complete(main_task)
            if not process_list.data.values:  # We haven't started anything yet
                accepted_command.append("boot")
        if cd:
            accepted_command_raw = cd.describe_fsm()
            accepted_command += [
                c.name.lower().replace("_", "-")
                for c in accepted_command_raw.data.commands
            ]
        logger.debug(f"Accepted commands: {accepted_command}")

        if command not in accepted_command and command != [sequence_commands[-1]]:
            logger.info(f"Skipping command '{command}'")
            continue

        logger.info(f"Running command: '{command}'")

        cmd_kwargs = {
            kwarg_name: kwargs[kwarg_name]
            for kwarg_name in cmd_to_options_and_args[command]
        }

        try:
            ctx.invoke(ctx.command.commands[command], **cmd_kwargs)
        except DruncException as e:
            logger.error(f"Error running command: '{command}'")
            logger.exception(e)
            raise e


def generate_fsm_sequence_command(ctx, sequence, controller_name):
    sequence_commands = []
    cmd_to_options_and_args = {}
    name_to_options_and_args = {}
    sequence_str = ""
    middle_text = "[optionally], then "
    command_ids = [command.id for command in sequence.sequence]

    if sequence.id == "start_run":
        command_ids = ["boot"] + command_ids
    elif sequence.id == "shutdown":
        command_ids = command_ids + ["terminate"]

    for command_id in command_ids:
        command_name = command_id.replace("_", "-")
        sequence_commands.append(command_name)
        sequence_str += f"{command_name}{middle_text}"
        if command_name not in ctx.command.commands.keys():
            raise DruncSetupException(
                f"Command {command_name} required by sequence {sequence.id} not found in the command list!"
            )

        params = ctx.command.commands[command_name].get_params(ctx)
        cmd_to_options_and_args[command_name] = []
        for param in params:
            if param.name == "help":
                continue
            cmd_to_options_and_args[command_name].append(param.name)
            name_to_options_and_args[param.name] = param

    sequence_str = sequence_str[: -len(middle_text)]

    cmd = partial(run_fsm_sequence, sequence_commands, cmd_to_options_and_args, ctx)
    cmd = click.pass_obj(cmd)

    for param_name, param in name_to_options_and_args.items():
        if param.name == "help":
            continue

        param_name = param_name.replace("_", "-").lower()
        cmd = click.option(
            f"--{param_name}",
            type=param.type,
            default=param.default,
            show_default=param.show_default,
            required=param.required,
            help=param.help,
        )(cmd)

    cmd = click.command(
        name=sequence.id.replace("_", "-"),
        help=f"Run the sequence {sequence.id}: {sequence_str}",
    )(cmd)

    return cmd, sequence.id.replace("_", "-")
