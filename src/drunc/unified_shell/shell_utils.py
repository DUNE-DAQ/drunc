from functools import partial

import click

from drunc.exceptions import DruncSetupException


def run_fsm_sequence(sequence_commands, options_and_args, ctx, **kwargs):
    print(f"Running sequence: {sequence_commands}")
    # params = cmd1, x=x, flag=flag
    for command in sequence_commands:
        print(f"Running command: {command}")
        cmd_kwargs = {
            kwarg_name: kwargs[kwarg_name] for kwarg_name in options_and_args[command]
        }
        ctx.command.invoke(ctx.command.commands[command], **cmd_kwargs)


def generate_fsm_sequence_command(ctx, sequence, controller_name):
    print(f"Sequence: {sequence.id}")
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
        sequence_commands.append(command_id)
        command_name = command_id.replace("_", "-")
        sequence_str += f"{command_name}{middle_text}"
        print(f"Considering command: {command_name}")
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

    cmd = partial(run_fsm_sequence, sequence_commands, cmd_to_options_and_args)
    cmd = click.pass_obj(cmd)

    for param_name, param in name_to_options_and_args.items():
        if param.name == "help":
            continue

        print(param_name)
        param_name = param_name.replace("_", "-").lower()
        cmd = click.option(
            f"--{param_name}",
            type=param.type,
            default=param.default,
            show_default=param.show_default,
            required=param.required,
            help=param.help,
        )(cmd)

    print(cmd.__dict__)
    cmd = click.command(
        name=sequence.id.replace("_", "-"),
        help=f"Run the sequence {sequence.id}: {sequence_str}",
    )(cmd)
    print(cmd.__dict__)
    return cmd, sequence.id.replace("_", "-")
