import click

from drunc.controller.interface.context import ControllerContext
from drunc.utils.shell_utils import add_traceback_flag


@click.command('describe')
@click.option("--command", type=str, default='.*')#, help='Which command you are interested')
@add_traceback_flag()
@click.pass_obj
def describe(obj:ControllerContext, command:str, traceback:bool) -> None:
    from druncschema.controller_pb2 import Argument

    if command == 'fsm':
        desc = obj.get_driver('controller').describe_fsm(rethrow=traceback)
    else:
        desc = obj.get_driver('controller').describe(rethrow=traceback)

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
@add_traceback_flag()
@click.pass_obj
def ls(obj:ControllerContext, traceback:bool) -> None:
    children = obj.get_driver('controller').ls(rethrow=traceback)
    if not children: return
    obj.print(children.text)


@click.command('status')
@add_traceback_flag()
@click.pass_obj
def status(obj:ControllerContext, traceback:bool) -> None:
    from druncschema.controller_pb2 import Status, ChildrenStatus
    status = obj.get_driver('controller').get_status(traceback)

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

    statuses = obj.get_driver('controller').get_children_status(traceback)

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
@add_traceback_flag()
@click.argument('controller_address', type=str)
@click.pass_obj
def connect(obj:ControllerContext, traceback:bool, controller_address:str) -> None:
    obj.print(f'Connecting this shell to it...')
    obj.set_controller_driver(controller_address, obj.print_traceback)
    from drunc.controller.interface.shell_utils import controller_setup
    controller_setup(obj, controller_address)



@click.command('take-control')
@add_traceback_flag()
@click.pass_obj
def take_control(obj:ControllerContext, traceback:bool) -> None:
    obj.get_driver('controller').take_control(traceback)


@click.command('surrender-control')
@add_traceback_flag()
@click.pass_obj
def surrender_control(obj:ControllerContext, traceback:bool) -> None:
    obj.get_driver('controller').surrender_control(traceback)


@click.command('who-am-i')
@click.pass_obj
def who_am_i(obj:ControllerContext) -> None:
    obj.print(obj.get_token().user_name)


@click.command('who-is-in-charge')
@add_traceback_flag()
@click.pass_obj
def who_is_in_charge(obj:ControllerContext, traceback:bool) -> None:
    who = obj.get_driver('controller').who_is_in_charge(traceback)
    if who:
        obj.print(who.text)


@click.command('fsm')
@add_traceback_flag()
@click.argument('command', type=str)
@click.argument('arguments', type=str, nargs=-1)
@click.pass_obj
def fsm(obj:ControllerContext, command, arguments, traceback:bool) -> None:
    from druncschema.controller_pb2 import FSMCommand

    if len(arguments) % 2 != 0:
        raise RuntimeError('Arguments are pairs of key-value!')
    desc = obj.get_driver('controller').describe_fsm(traceback)

    from drunc.controller.interface.shell_utils import search_fsm_command, validate_and_format_fsm_arguments, ArgumentException

    command_desc = search_fsm_command(command, desc.commands)
    if command_desc is None:
        obj.error(f'Command "{command}" does not exist, or is not accessible right now')
        return

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
            rethrow = True, # we throw here any way
        )
    except ArgumentException as ae:
        obj.print(str(ae))
        return
    except Exception as e:
        obj.error(e)
        if traceback:
            raise e

    if not result: return

    from drunc.controller.interface.shell_utils import format_bool, tree_prefix
    from druncschema.controller_pb2 import FSMCommandResponseCode

    from rich.table import Table
    t = Table(title=f'{command} execution report')
    t.add_column('Name')
    t.add_column('Command success')
    t.add_row(
        '<root>',
        format_bool(result.successful == FSMCommandResponseCode.SUCCESSFUL),
    )

    i=0
    n=len(result.children_successful)
    for name, sucess in result.children_successful.items():
        t.add_row(
            tree_prefix(i, n)+name,
            format_bool(sucess == FSMCommandResponseCode.SUCCESSFUL),
        )
        i += 1
    obj.print(t)


@click.command('include')
@add_traceback_flag()
@click.pass_obj
def include(obj:ControllerContext, traceback:bool) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'include',
    )
    result = obj.get_driver('controller').include(rethrow=traceback, arguments=data)
    if not result: return
    obj.print(result.text)


@click.command('exclude')
@add_traceback_flag()
@click.pass_obj
def exclude(obj:ControllerContext, traceback:bool) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'exclude',
    )
    result = obj.get_driver('controller').exclude(rethrow=traceback, arguments=data)
    if not result: return
    obj.print(result.text)
