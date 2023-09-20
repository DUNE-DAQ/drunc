import click
import click_shell
from drunc.controller.utils import send_command
from drunc.utils.grpc_utils import unpack_any
from druncschema.generic_pb2 import PlainText, PlainTextVector
from drunc.utils.utils import CONTEXT_SETTINGS, log_levels

class ControllerContext:
    def __init__(self, print_traceback:bool=False) -> None:
        from rich.console import Console
        self._console = Console()
        from logging import getLogger
        self.log = getLogger("ControllerShell")
        self.print_traceback = print_traceback
        self.controller = None
        self.status_receiver = None

        import getpass
        user=getpass.getuser()

        from druncschema.token_pb2 import Token
        self.token = Token ( # fake token, but should be figured out from the environment/authoriser
            token = f'{user}-token',
            user_name = user
        )

        self.status_receiver = None
        self.took_control = False

    def start_listening(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.conf_types import ConfTypes
        # from drunc.utils.grpc_utils import unpack_any

        self.status_receiver = BroadcastHandler(
            broadcast_configuration = broadcaster_conf,
            conf_type = ConfTypes.Protobuf
        )


    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()

    def print(self, text):
        self._console.print(text)

    def rule(self, text):
        self._console.rule(text)


@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('controller-address', type=str)#, help='Which address the controller is running on')
@click.option('-t', '--traceback', is_flag=True, default=True, help='Print full exception traceback')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_context
def controller_shell(ctx, controller_address:str, log_level:str, traceback:bool) -> None:#, this_port:int, just_watch:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj = ControllerContext(
        print_traceback = traceback
    )

    # first add the shell to the controller broadcast list
    from druncschema.controller_pb2_grpc import ControllerStub
    import grpc

    channel = grpc.insecure_channel(controller_address)

    ctx.obj.controller = ControllerStub(channel)

    ctx.obj.log.info('Connected to the controller')

    from druncschema.request_response_pb2 import Description
    desc = Description()

    ntries = 5

    from drunc.utils.grpc_utils import ServerUnreachable
    for itry in range(ntries):
        try:
            response = send_command(
                controller = ctx.obj.controller,
                token = ctx.obj.token,
                command = 'describe',
                rethrow = True
            )

            response.data.Unpack(desc)

        except ServerUnreachable as e:
            if itry+1 == ntries:
                raise e
            else:
                ctx.obj.log.error(f'Could not connect to the controller, trial {itry+1} of {ntries}')
                from time import sleep
                sleep(0.5)

        except Exception as e:
            ctx.obj.log.critical('Could not get the controller\'s status')
            ctx.obj.log.critical(e)
            ctx.obj.log.critical('Exiting.')
            ctx.obj.terminate()
            raise e

        else:
            ctx.obj.log.info(f'{controller_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
            ctx.obj.start_listening(desc.broadcast)
            break


    ctx.obj.log.info('Attempting to list this controller\'s children')
    from druncschema.generic_pb2 import PlainText, PlainTextVector

    response = send_command(
        controller = ctx.obj.controller,
        token = ctx.obj.token,
        command = 'ls',
        rethrow = True
    )

    ptv = PlainTextVector()
    response.data.Unpack(ptv)
    ctx.obj.log.info(f'{desc.name}.{desc.session}\'s children: {ptv.text}')


    def cleanup():
        # remove the shell from the controller broadcast list
        dead = False

        from drunc.utils.grpc_utils import unpack_any
        try:
            response = send_command(
                controller = ctx.obj.controller,
                token = ctx.obj.token,
                command = 'who_is_in_charge',
                rethrow = True
            )
            pt = unpack_any(response.data, PlainText).text
        except grpc.RpcError as e:
            dead = grpc.StatusCode.UNAVAILABLE == e.code()
        except Exception as e:
            ctx.obj.log.error('Could not understand who is in charge from the controller.')
            ctx.obj.log.error(e)
            pt = 'no_one'

        if dead:
            ctx.obj.log.error('Controller is dead. Exiting.')
            return

        if pt == ctx.obj.token.user_name and ctx.obj.took_control:
            ctx.obj.log.info('You are in control. Surrendering control.')
            try:
                response = send_command(
                    controller = ctx.obj.controller,
                    token = ctx.obj.token,
                    command = 'surrender_control',
                    rethrow = True
                )
            except Exception as e:
                ctx.obj.log.error('Could not surrender control.')
                ctx.obj.log.error(e)
            ctx.obj.log.info('Control surrendered.')
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)

    ctx.obj.log.info(f'Taking control of the controller as {ctx.obj.token}')
    try:
        response = send_command(
            controller = ctx.obj.controller,
            token = ctx.obj.token,
            command = 'take_control',
            rethrow = True
        )
        ctx.obj.took_control = True

    except Exception as e:
        ctx.obj.log.error('You are NOT in control.')
        ctx.obj.took_control = False
        #raise e
        return

    ctx.obj.log.info('You are in control.')


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
                controller = obj.controller,
                token = obj.token,
                command = 'describe_fsm',
                data = None
            ).data,
            FSMCommandsDescription
        )
    else:
        desc = unpack_any(
            send_command(
                controller = obj.controller,
                token = obj.token,
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
        from druncschema.generic_pb2 import string_msg, float_msg, int_msg
        from drunc.utils.grpc_utils import unpack_any

        if arg.HasField('default_value'):
            if arg.type == Argument.Type.STRING:
                d = unpack_any(arg.default_value, string_msg).value

            elif arg.type == Argument.Type.FLOAT:
                d = str(unpack_any(arg.default_value, float_msg).value)

            elif arg.type == Argument.Type.INT:
                d = str(unpack_any(arg.default_value, int_msg).value)

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
            controller = obj.controller,
            token = obj.token,
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
            controller = obj.controller,
            token = obj.token,
            command = 'get_status',
            data = None
        ).data,
        Status
    )

    def format_bool(b, format=['dark_green', 'bold white on red'], false_is_good = False):
        index_true = 0 if not false_is_good else 1
        index_false = 1 if not false_is_good else 0

        return f'[{format[index_true]}]Yes[/]' if b else f'[{format[index_false]}]No[/]'

    from rich.table import Table
    t = Table(title=f'{status.name} status')
    t.add_column('Name')
    t.add_column('State')
    t.add_column('In error', justify='center')
    t.add_column('Included', justify='center')
    t.add_row(
        status.name,
        status.state,
        format_bool(status.in_error, false_is_good = True),
        format_bool(status.included),
    )

    statuses = unpack_any(
        send_command(
            controller = obj.controller,
            token = obj.token,
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
        first_column = ''
        if i==0 and how_many == 1:
            first_column = first_one+c_status.name
        elif i==0:
            first_column = first_many+c_status.name
        elif i == how_many-1:
            first_column = last+c_status.name
        else:
            first_column = next+c_status.name

        t.add_row(
            first_column,
            c_status.state,
            format_bool(c_status.in_error, false_is_good=True),
            format_bool(c_status.included)
        )
    obj.print(t)


@controller_shell.command('take-control')
@click.pass_obj
def take_control(obj:ControllerContext) -> None:
    send_command(
        controller = obj.controller,
        token = obj.token,
        command = 'take_control',
        data = None
    )


@controller_shell.command('surrender-control')
@click.pass_obj
def surrender_control(obj:ControllerContext) -> None:
    send_command(
        controller = obj.controller,
        token = obj.token,
        command = 'surrender_control',
        data = None
    )


@controller_shell.command('who-am-i')
@click.pass_obj
def who_am_i(obj:ControllerContext) -> None:
    obj.print(obj.token.user_name)


@controller_shell.command('who-is-in-charge')
@click.pass_obj
def who_is_in_charge(obj:ControllerContext) -> None:

    who = unpack_any(
        send_command(
            controller = obj.controller,
            token = obj.token,
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

    from druncschema.controller_pb2 import FSMCommandsDescription, Argument
    desc = unpack_any(
        send_command(
            controller = obj.controller,
            token = obj.token,
            command = 'describe_fsm',
            data = None
        ).data,
        FSMCommandsDescription
    )

    from drunc.controller.interface.shell_utils import search_fsm_command, validate_and_format_fsm_arguments, ArgumentException

    command_desc = search_fsm_command(command, desc.commands)
    if command_desc is None:
        obj.log.error(f'Command "{command}" does not exist, or is not accessible right now')
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
        send_command(
            controller = obj.controller,
            token = obj.token,
            command = 'execute_fsm_command',
            data = data
        )
    except ArgumentException as ae:
        obj.print(str(ae))
    except Exception as e:
        raise e




@controller_shell.command('include')
@click.pass_obj
def some_command(obj:ControllerContext) -> None:
    from druncschema.controller_pb2 import FSMCommand
    data = FSMCommand(
        command_name = 'include',
    )
    send_command(
        controller = obj.controller,
        token = obj.token,
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
        controller = obj.controller,
        token = obj.token,
        command = 'exclude',
        data = data
    )

