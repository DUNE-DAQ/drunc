import click
from drunc.controller.interface.context import ControllerContext

@click.command('describe')
@click.option("--command", type=str, default='.*')#, help='Which command you are interested')
@click.pass_obj
def describe(obj:ControllerContext, command:str) -> None:
    from druncschema.controller_pb2 import Argument
    from drunc.utils.shell_utils import InterruptedCommand

    if command == 'fsm':
        desc = obj.get_driver('controller').describe_fsm().data
    else:
        desc = obj.get_driver('controller').describe().data

    if not desc: return

    from rich.table import Table
    t = Table(title=f'{desc.name}.{desc.session} ({desc.type}) commands')
    t.add_column('name')
    t.add_column('input type')
    t.add_column('return type')
    t.add_column('help')

    if command == 'fsm':
        t.add_column('Command arguments')

    from drunc.utils.utils import regex_match

    def format_fsm_argument(arg):
        d = '<no_default>'
        from druncschema.generic_pb2 import string_msg, float_msg, int_msg, bool_msg
        from drunc.utils.grpc_utils import unpack_any

        if arg.HasField('default_value'):
            if arg.type == Argument.Type.STRING:
                d = unpack_any(arg.default_value, string_msg).value

            elif arg.type == Argument.Type.FLOAT:
                d = str(unpack_any(arg.default_value, float_msg).value)

            elif arg.type == Argument.Type.INT:
                d = str(unpack_any(arg.default_value, int_msg).value)

            elif arg.type == Argument.Type.BOOL:
                d = str(unpack_any(arg.default_value, bool_msg).value)

            else:
                d = arg.default_value

        return f'{arg.name} ({Argument.Type.Name(arg.type)} {Argument.Presence.Name(arg.presence)}) default: {d} help: {arg.help}'

    for c in desc.commands:

        if not regex_match(command, c.name) and command != 'fsm':
            continue

        if command == 'fsm':
            args = c.arguments
            if len(args) == 0:
                t.add_row(c.name, ','.join(c.data_type), c.return_type, c.help,)
            elif len(args) == 1:
                t.add_row(c.name, ','.join(c.data_type), c.return_type, c.help,format_fsm_argument(args[0]))
            else:
                t.add_row(c.name, ','.join(c.data_type), c.return_type, c.help,format_fsm_argument(args[0]))
                for i in range(1, len(args)):
                    t.add_row('', '', '', '', format_fsm_argument(args[i]))

        else:
            t.add_row(c.name, ','.join(c.data_type), c.return_type, c.help)


    obj.print(t)



@click.command('ls')
@click.pass_obj
def ls(obj:ControllerContext) -> None:
    children = obj.get_driver('controller').ls().data
    if not children: return
    obj.print(children.text)


@click.command('status')
@click.pass_obj
def status(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import Status, ChildrenStatus
    status = obj.get_driver('controller').get_status().data

    if not status: return

    from drunc.controller.interface.shell_utils import format_bool, tree_prefix

    from rich.table import Table
    t = Table(title=f'{status.name} status')
    t.add_column('Name')
    t.add_column('State')
    t.add_column('Substate')
    t.add_column('In error', justify='center')
    t.add_column('Included', justify='center')
    t.add_row(
        status.name,
        status.state,
        status.sub_state,
        format_bool(status.in_error, false_is_good = True),
        format_bool(status.included),
    )

    statuses = obj.get_driver('controller').get_children_status().data

    if not statuses:
        statuses = []

    how_many = len(statuses.children_status)

    for i, c_status in enumerate(statuses.children_status):
        first_column = tree_prefix(i, how_many)+c_status.name

        t.add_row(
            first_column,
            c_status.state,
            c_status.sub_state,
            format_bool(c_status.in_error, false_is_good=True),
            format_bool(c_status.included)
        )
    obj.print(t)

@click.command('connect')
@click.argument('controller_address', type=str)
@click.pass_obj
def connect(obj:ControllerContext, controller_address:str) -> None:
    obj.print(f'Connecting this shell to it...')
    from drunc.exceptions import DruncException

    obj.set_controller_driver(controller_address)
    from drunc.controller.interface.shell_utils import controller_setup
    controller_setup(obj, controller_address)


@click.command('take-control')
@click.pass_obj
def take_control(obj:ControllerContext) -> None:
    obj.get_driver('controller').take_control().data


@click.command('surrender-control')
@click.pass_obj
def surrender_control(obj:ControllerContext) -> None:
    obj.get_driver('controller').surrender_control().data


@click.command('who-am-i')
@click.pass_obj
def who_am_i(obj:ControllerContext) -> None:
    obj.print(obj.get_token().user_name)


@click.command('who-is-in-charge')
@click.pass_obj
def who_is_in_charge(obj:ControllerContext) -> None:
    who = obj.get_driver('controller').who_is_in_charge().data
    if who:
        obj.print(who.text)


@click.command('fsm')
@click.argument('fsm_command', type=str, nargs=-1)
@click.pass_obj
def fsm(obj:ControllerContext, fsm_command:str) -> None:
    from drunc.controller.interface.shell_utils import format_bool, tree_prefix, search_fsm_command, validate_and_format_fsm_arguments, ArgumentException
    from drunc.utils.grpc_utils import unpack_any
    from druncschema.controller_pb2 import FSMResponseFlag, FSMCommandResponse, FSMCommand, FSMCommandDescription
    from druncschema.request_response_pb2 import ResponseFlag
    from rich.table import Table

    def split_FSM_args(obj:ControllerContext, passed_commands:tuple):
        # Note this is a placeholder - want to get this from OKS.
        available_commands = ["conf", "start", "enable_triggers", "disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "start_run", "stop_run", "shutdown"]
        available_command_mandatory_args = [[], ["run_number"], [], [], [], [], []]
        available_command_opt_args = []
        available_args = ["run_number"]

        # Get the index of all the commands in the command str
        command_list = [command for command in passed_commands if command in available_commands]
        if len(command_list) == 0:
                if len(passed_commands) == 1:
                    obj.print(f"The passed command [red]{passed_commands}[/red] was not understood.")    
                else:
                    obj.print(f"None of the passed arguments were correctly identified.")
                raise SystemExit(1)

        command_index = [passed_commands.index(command) for command in command_list]

        # Get the arguments for each command
        command_index.append(-1)
        command_argument_list = []
        for i in range(len(command_index)-2):
            command_argument_list.append(list(passed_commands[command_index[i]+1:command_index[i + 1]]))
        command_argument_list.append(list(passed_commands[command_index[-2]+1:]))
        print(f"{command_argument_list=}")
        # Not elegant, would be better to check at the command level first but it does work
        for argument_list in command_argument_list:
            argument_names = argument_list[::2]
            for argument in argument_names:
                # Check if the argument is legal
                if argument not in available_args:
                    from drunc.controller.exceptions import MalformedCommandArgument
                    raise MalformedCommandArgument(f"Argument '{argument}' not recognised as a valid argument.")
                # Check for duplicates
                if argument_names.count(argument) != 1:
                    from drunc.controller.exceptions import MalformedCommand
                    raise MalformedCommand(f"Argument '{argument}' has been repeated.")


        # Extract commands from sequences
        for command in command_list:
            if command not in ["start_run", "stop_run", "shutdown"]:
                continue
            sequence_command_index = command_list.index(command)
            sequence_command_args = command_argument_list[sequence_command_index]
            match command:
                case "start_run":
                    sequence_commands = ["conf", "start", "enable_triggers"]
                case "stop_run":
                    sequence_commands = ["disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop"]
                case "shutdown":
                    sequence_commands = ["disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "scrap"]
            del command_list[sequence_command_index]
            command_list[sequence_command_index:sequence_command_index] = sequence_commands

            for _ in range(len(sequence_commands)-1):
                command_argument_list.insert(sequence_command_index, list(sequence_command_args))

        # Check the mandatory arguments
        for command in command_list:
            mandatory_command_arguments = available_command_mandatory_args[available_commands.index(command)]
            provided_command_arguments = command_argument_list[command_list.index(command)]
            if mandatory_command_arguments == []:
                continue
            for argument in mandatory_command_arguments:
                if argument not in provided_command_arguments:
                    missing_command_arguments = list(set(mandatory_command_arguments) - set(provided_command_arguments))
                    obj.print(f"There are missing arguments for command [green]{command}[/green]. Missing arguments: [red]{' '.join(missing_command_arguments)}[/red].")
                    raise SystemExit(1)

        return command_list, command_argument_list


    def dict_arguments(arguments:str) -> dict:
        if len(arguments) % 2 != 0:
            raise click.BadParameter('Arguments are pairs of key-value!')
        keys = arguments[::2]
        values = arguments[1::2]
        arguments_dict = {keys[i]:values[i] for i in range(len(keys))}
        return arguments_dict

    def bool_to_success(flag_message, FSM):
        flag = False
        if FSM and flag_message == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY:
            flag = True
        if not FSM and flag_message == ResponseFlag.EXECUTED_SUCCESSFULLY:
            flag = True
        return "success" if flag else "failed"

    def add_to_table(table, response, prefix=''):
        table.add_row(
            prefix+response.name,
            bool_to_success(response.flag, FSM=False),
            bool_to_success(response.data.flag, FSM=True) if response.flag == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY else "failed",
        )
        for child_response in response.children:
            add_to_table(table, child_response, "  "+prefix)

    def print_execution_report(command:str, result:FSMCommandResponse) -> None:
        t = Table(title=f'{command} execution report')
        t.add_column('Name')
        t.add_column('Command execution')
        t.add_column('FSM transition')

        add_to_table(t, result)
        obj.print(t)
        obj.print("") # For formatting
        return

    def print_status_summary(obj:ControllerContext) -> None:
        status = obj.get_driver('controller').get_status().data.state
        available_actions = [command.name for command in obj.get_driver('controller').describe_fsm().data.commands]
        obj.print(f"Current FSM status is [green]{status}[/green]. Available transitions are [green]{' '.join(available_actions)}[/green]")
        return

    def filter_arguments(arguments:dict, fsm_command:FSMCommandDescription) -> dict:
        if not arguments:
            return None
        cmd_arguments = {}
        command_arguments = fsm_command.arguments
        cmd_argument_names = [argument.name for argument in command_arguments]
        for argument in list(arguments):
            if argument in cmd_argument_names:
                cmd_arguments[argument] = arguments[argument]
        return cmd_arguments

    def construct_FSM_command(obj:ControllerContext, command:tuple[str, list]) -> FSMCommand:
        command_name = command[0]
        print(f"{command[1]=}")
        command_args = dict_arguments(command[1])
        command_desc = search_fsm_command(command_name, obj.get_driver('controller').describe_fsm().data.commands) # FSMCommandDescription
        if command_desc == None:
            return None

        # Apply the appropriate arguments for this command
        arguments = filter_arguments(command_args, command_desc)
        # Construct the FSMCommand
        from druncschema.controller_pb2 import FSMCommand
        cmd = FSMCommand( 
            command_name = command_name, 
            arguments = validate_and_format_fsm_arguments(arguments, command_desc.arguments)
        )
        return cmd

    def send_FSM_command(obj:ControllerContext, command:FSMCommand) -> FSMCommandResponse:
        result = None
        try:
            result = obj.get_driver('controller').execute_fsm_command(command)
        except Exception as e: # TODO narrow this exception down
            obj.print(f"[red]{command.command_name}[/red] failed.")
            raise e
        print_execution_report(command.command_name, result)
        obj.print(f"[green]{command.command_name}[/green] executed successfully.")
        # if not result: return
        return result

    # Split command into a list of commands and a list of arguments
    command_list, argument_list = split_FSM_args(obj, fsm_command)
    print(f"{argument_list=}")
    # Execute the FSM commands
    result = None
    for command in zip(command_list, argument_list):
        grpc_command = construct_FSM_command(obj, command)
        if grpc_command == None:
            obj.print(f"[red]{command[0]}[/red] is not possible in current state, not executing.")
            continue
        obj.print(f"Sending [green]{command[0]}[/green].")
        result = send_FSM_command(obj, grpc_command)
        if (result == None):
            obj.print(f"Transition {FSMtransition} did not execute")
            break

    print_status_summary(obj)
    return

@click.command('include')
@click.pass_obj
def include(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'include',
    )
    result = obj.get_driver('controller').include(arguments=data).data
    if not result: return
    obj.print(result.text)


@click.command('exclude')
@click.pass_obj
def exclude(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'exclude',
    )
    result = obj.get_driver('controller').exclude(arguments=data).data
    if not result: return
    obj.print(result.text)
