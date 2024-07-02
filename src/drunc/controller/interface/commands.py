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
@click.argument('command', type=str)
@click.argument('arguments', type=str, nargs=-1)
@click.option('-b', '--batch-mode', type=bool, default=False, help='Allow for a sequence of fsm commands')
@click.pass_obj
def fsm(obj:ControllerContext, command:str, arguments:str, batch_mode=False) -> None:
    from drunc.controller.interface.shell_utils import format_bool, tree_prefix, search_fsm_command, validate_and_format_fsm_arguments, ArgumentException
    from drunc.utils.grpc_utils import unpack_any
    from druncschema.controller_pb2 import FSMResponseFlag, FSMCommandResponse, FSMCommand
    from rich.table import Table
    
    if len(arguments) % 2 != 0:
        raise click.BadParameter('Arguments are pairs of key-value!')

    from druncschema.request_response_pb2 import ResponseFlag
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
        return

    def construct_send_FSM_command(obj:ControllerContext, command:str, arguments:str = ""):
        desc = obj.get_driver('controller').describe_fsm().data # desc is [FSMCommandDescription, ...] for each possible transition from the current FSMconfiguration.state
        command_desc = search_fsm_command(command, desc.commands) # Search the commands retrieved by describe_fsm().data, return a grpc::FSMCommandDescription of command
        if command_desc is None:
            obj.error(f'Command "{command}" does not exist, or is not accessible right now')
            return # TODO change the type of exception

        keys = arguments[::2]
        values = arguments[1::2]
        arguments_dict = {keys[i]:values[i] for i in range(len(keys))}
        result = None
        try:
            formated_args = validate_and_format_fsm_arguments(arguments_dict, command_desc.arguments)
            data = FSMCommand(
                command_name = command,
                arguments = formated_args,
            )
            result = obj.get_driver('controller').execute_fsm_command(
                arguments = data,
            )
        except ArgumentException as ae:
            obj.print(str(ae))
            return # TODO propagate this through

        print_execution_report(command, result)
        if not result: return
        return result

    # Create a new list for all the FSM commands
    
    if command in ["start_run", "stop_run", "shutdown"]: # FSMsequence is provided
        match command:
        # TODO - make this come from the XML rather than being hard coded.
            case "start_run":
                FSMtransitions = ["conf", "start", "enable_triggers"]
            case "stop_run":
                FSMtransitions = ["disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop"]
            case "shutdown":
                FSMtransitions = ["disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "scrap"]
    elif command in ["conf", "start", "enable_triggers", "disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "scrap"]: # FSMtransition is provided
        FSMtransitions = [command]
    else:
        raise click.BadParameter('Unrecognised FSMtransition.')

    # Execute all the FSM commands
    for FSMtransition in FSMtransitions:
        result = construct_send_FSM_command(obj, FSMtransition, arguments)
        if (result == None):
            obj.print(f"Remaining commands not executed. Last command attempted: fsm {FSMtransition}")
            break

    # If the last command failed, don't print the summary table
    if result != None:
        print_execution_report(command, result)
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
