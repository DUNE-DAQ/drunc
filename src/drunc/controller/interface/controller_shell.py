import click
import click_shell
from drunc.controller.utils import send_command
from drunc.utils.grpc_utils import unpack_any
from drunc.utils.shell_utils import ShellContext
from druncschema.generic_pb2 import PlainText, PlainTextVector
from druncschema.token_pb2 import Token

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels
from drunc.utils.shell_utils import GRPCDriver
from typing import Mapping
from drunc.controller.controller_driver import ControllerDriver

class ControllerContext(ShellContext): # boilerplatefest
    status_receiver = None

    def reset(self, address:str=None, print_traceback:bool=False):
        self.address = address
        super(ControllerContext, self)._reset(
            print_traceback = print_traceback,
            name = 'controller_context',
            token_args = {},
            driver_args = {},
        )

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}
        return {
            'controller_driver': ControllerDriver(
                self.address,
                self._token
            )
        }

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname
        return create_dummy_token_from_uname()


    def start_listening(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.conf_types import ConfTypes

        self.status_receiver = BroadcastHandler(
            broadcast_configuration = broadcaster_conf,
            conf_type = ConfTypes.Protobuf
        )

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()



@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('controller-address', type=str)
@click.option('-t', '--traceback', is_flag=True, default=False, help='Print full exception traceback')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_context
def controller_shell(ctx, controller_address:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj.reset(
        print_traceback = traceback,
        address = controller_address,
    )

    ctx.obj.info('Connected to the controller')

    from druncschema.request_response_pb2 import Description
    desc = Description()

    ntries = 5

    from drunc.utils.grpc_utils import ServerUnreachable
    for itry in range(ntries):
        try:
            desc = ctx.obj.get_driver().describe()
            # response = send_command(
            #     controller = ctx.obj.get_driver().stub,
            #     token = ctx.obj.get_token(),
            #     command = 'ls',
            #     rethrow = True
            # )
            # response.data.unpack(desc)
        except ServerUnreachable as e:
            if itry+1 == ntries:
                raise e
            else:
                ctx.obj.error(f'Could not connect to the controller, trial {itry+1} of {ntries}')
                from time import sleep
                sleep(0.5)

        except Exception as e:
            ctx.obj.critical('Could not get the controller\'s status')
            ctx.obj.critical(e)
            ctx.obj.critical('Exiting.')
            ctx.obj.terminate()
            raise e

        else:
            ctx.obj.info(f'{controller_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
            ctx.obj.start_listening(desc.broadcast)
            break


    ctx.obj.info('Attempting to list this controller\'s children')
    from druncschema.generic_pb2 import PlainText, PlainTextVector

    response = send_command(
        controller = ctx.obj.get_client_side_object(),
        token = ctx.obj.get_token(),
        command = 'ls',
        rethrow = True
    )

    ptv = PlainTextVector()
    response.data.Unpack(ptv)
    ctx.obj.info(f'{desc.name}.{desc.session}\'s children: {ptv.text}')


    def cleanup():
        # remove the shell from the controller broadcast list
        dead = False

        from drunc.utils.grpc_utils import unpack_any
        try:
            response = send_command(
                controller = ctx.obj.get_client_side_object(),
                token = ctx.obj.get_token(),
                command = 'who_is_in_charge',
                rethrow = True
            )
            pt = unpack_any(response.data, PlainText).text
        except grpc.RpcError as e:
            dead = grpc.StatusCode.UNAVAILABLE == e.code()
        except Exception as e:
            ctx.obj.error('Could not understand who is in charge from the controller.')
            ctx.obj.error(e)
            pt = 'no_one'

        if dead:
            ctx.obj.error('Controller is dead. Exiting.')
            return

        if pt == ctx.obj.get_token().user_name and ctx.obj.took_control:
            ctx.obj.info('You are in control. Surrendering control.')
            try:
                response = send_command(
                    controller = ctx.obj.get_client_side_object(),
                    token = ctx.obj.get_token(),
                    command = 'surrender_control',
                    rethrow = True
                )
            except Exception as e:
                ctx.obj.error('Could not surrender control.')
                ctx.obj.error(e)
            ctx.obj.info('Control surrendered.')
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)

    ctx.obj.info(f'Taking control of the controller as {ctx.obj.get_token()}')
    try:
        response = send_command(
            controller = ctx.obj.get_client_side_object(),
            token = ctx.obj.get_token(),
            command = 'take_control',
            rethrow = True
        )
        ctx.obj.took_control = True

    except Exception as e:
        ctx.obj.error('You are NOT in control.')
        ctx.obj.took_control = False
        #raise e
        return

    ctx.obj.info('You are in control.')


@controller_shell.command('describe')
@click.option("--command", type=str, default='.*')#, help='Which command you are interested')
@click.pass_obj
def describe(obj:ControllerContext, command) -> None:
    from druncschema.request_response_pb2 import Description
    from druncschema.controller_pb2 import FSMCommandsDescription, Argument
    desc = Description()
    if command == 'fsm':
        desc = unpack_any(
            send_command(
                controller = obj.get_client_side_object(),
                token = obj.get_token(),
                command = 'describe_fsm',
                data = None
            ).data,
            FSMCommandsDescription
        )
    else:
        desc = unpack_any(
            send_command(
                controller = obj.get_client_side_object(),
                token = obj.get_token(),
                command = 'describe',
                data = None
            ).data,
            Description
        )

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



@controller_shell.command('ls')
@click.pass_obj
def ls(obj:ControllerContext) -> None:
    children = unpack_any(
        send_command(
            controller = obj.get_client_side_object(),
            token = obj.get_token(),
            command = 'ls',
            data = None
        ).data,
        PlainTextVector
    )
    obj.print(children.text)

@controller_shell.command('status')
@click.pass_obj
def status(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import Status, ChildrenStatus

    status = unpack_any(
        send_command(
            controller = obj.get_client_side_object(),
            token = obj.get_token(),
            command = 'get_status',
            data = None
        ).data,
        Status
    )
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

    statuses = unpack_any(
        send_command(
            controller = obj.get_client_side_object(),
            token = obj.get_token(),
            command = 'get_children_status',
            data = None
        ).data,
        ChildrenStatus
    )

    first_one = "└── "
    first_many = "├── "
    next = "├── "
    last = "└── "

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


@controller_shell.command('take-control')
@click.pass_obj
def take_control(obj:ControllerContext) -> None:
    send_command(
        controller = obj.get_client_side_object(),
        token = obj.get_token(),
        command = 'take_control',
        data = None
    )


@controller_shell.command('surrender-control')
@click.pass_obj
def surrender_control(obj:ControllerContext) -> None:
    send_command(
        controller = obj.get_client_side_object(),
        token = obj.get_token(),
        command = 'surrender_control',
        data = None
    )


@controller_shell.command('who-am-i')
@click.pass_obj
def who_am_i(obj:ControllerContext) -> None:
    obj.print(obj.get_token().user_name)


@controller_shell.command('who-is-in-charge')
@click.pass_obj
def who_is_in_charge(obj:ControllerContext) -> None:

    who = unpack_any(
        send_command(
            controller = obj.get_client_side_object(),
            token = obj.get_token(),
            command = 'who_is_in_charge',
            data = None
        ).data,
        PlainText
    )
    obj.print(who.text)


@controller_shell.command('fsm')
@click.argument('command', type=str)
@click.argument('arguments', type=str, nargs=-1)
@click.pass_obj
def fsm(obj:ControllerContext, command, arguments) -> None:
    from druncschema.controller_pb2 import FSMCommand

    if len(arguments) % 2 != 0:
        raise RuntimeError('Arguments are pairs of key-value!')
    from drunc.utils.grpc_utils import unpack_any
    from druncschema.controller_pb2 import FSMCommandsDescription, Argument, FSMCommandResponse, FSMCommandResponseCode
    desc = unpack_any(
        send_command(
            controller = obj.get_client_side_object(),
            token = obj.get_token(),
            command = 'describe_fsm',
            data = None
        ).data,
        FSMCommandsDescription
    )

    from drunc.controller.interface.shell_utils import search_fsm_command, validate_and_format_fsm_arguments, ArgumentException

    command_desc = search_fsm_command(command, desc.commands)
    if command_desc is None:
        obj.error(f'Command "{command}" does not exist, or is not accessible right now')
        return

    keys = arguments[::2]
    values = arguments[1::2]
    arguments_dict = {keys[i]:values[i] for i in range(len(keys))}
    try:
        formated_args = validate_and_format_fsm_arguments(arguments_dict, command_desc.arguments)
        data = FSMCommand(
            command_name = command,
            arguments = formated_args,
        )
        r = send_command(
            controller = obj.get_client_side_object(),
            token = obj.get_token(),
            command = 'execute_fsm_command',
            data = data
        )
        result = unpack_any(r.data, FSMCommandResponse)
    except ArgumentException as ae:
        obj.print(str(ae))
        return
    except Exception as e:
        raise e

    from drunc.controller.interface.shell_utils import format_bool, tree_prefix

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


@controller_shell.command('include')
@click.pass_obj
def some_command(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'include',
    )

    send_command(
        controller = obj.get_client_side_object(),
        token = obj.get_token(),
        command = 'include',
        data = data
    )

@controller_shell.command('exclude')
@click.pass_obj
def some_command(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'exclude',
    )

    send_command(
        controller = obj.get_client_side_object(),
        token = obj.get_token(),
        command = 'exclude',
        data = data
    )

