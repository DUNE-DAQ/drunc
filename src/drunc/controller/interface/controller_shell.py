import click
import click_shell
from drunc.controller.utils import send_command
from drunc.utils.grpc_utils import unpack_any
from druncschema.generic_pb2 import PlainText, PlainTextVector
from drunc.utils.utils import CONTEXT_SETTINGS, log_levels

class ControllerContext:
    def __init__(self,  ctler_conf:str=None, print_traceback:bool=False) -> None:
        from rich.console import Console
        self._console = Console()
        from logging import getLogger
        self.log = getLogger("ControllerShell")
        self.print_traceback = True
        self.controller = None

        import os
        user = os.getlogin()

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

        # KafkaStdoutBroadcastHandler(
        #     conf = data,
        #     conf_type = 'protobuf',
        #     message_format = BroadcastMessage,
        # )


    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()

    def print(self, text):
        self._console.print(text)

    def rule(self, text):
        self._console.rule(text)


@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('controller-address', type=str)#, help='Which address the controller is running on')
# @click.argument('this-port', type=int)#, help='Which port to use for receiving status')
# @click.option('--just-watch', type=bool, default=False, is_flag=True, help='If one just doesn\'t want to take control of the controller')
# @click.argument('conf', type=click.Path(exists=True))
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_context
def controller_shell(ctx, controller_address:str, log_level:str) -> None:#, this_port:int, just_watch:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj = ControllerContext()

    # first add the shell to the controller broadcast list
    from druncschema.controller_pb2_grpc import ControllerStub
    import grpc

    channel = grpc.insecure_channel(controller_address)

    ctx.obj.controller = ControllerStub(channel)

    ctx.obj.log.info('Connected to the controller')

    from druncschema.request_response_pb2 import Description
    desc = Description()

    try:
        response = send_command(
            controller = ctx.obj.controller,
            token = ctx.obj.token,
            command = 'describe',
            rethrow = True
        )

        response.data.Unpack(desc)

    except Exception as e:
        ctx.obj.log.error('Could not get the controller\'s status')
        ctx.obj.log.error(e)
        ctx.obj.log.error('Exiting.')
        ctx.obj.terminate()
        raise e

    ctx.obj.log.info(f'{controller_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    ctx.obj.start_listening(desc.broadcast)

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


@controller_shell.command('man')
@click.option("--command", type=str, default='.*')#, help='Which command you are interested')
@click.pass_obj
def man(obj:ControllerContext, command) -> None:
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

        if arg.HasField('default_value'):
            if arg.type == Argument.Type.STRING:
                from drunc.utils.grpc_utils import unpack_any
                d = unpack_any(arg.default_value, string_msg).value

            elif arg.type == Argument.Type.FLOAT:
                from drunc.utils.grpc_utils import unpack_any
                d = str(unpack_any(arg.default_value, float_msg).value)

            elif type == Argument.Type.INT:
                from drunc.utils.grpc_utils import unpack_any
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
    def format_bool(b):
        return 'Y' if b else 'N'
    from rich.table import Table
    t = Table(title=f'{status.name} status')
    t.add_column('name')
    t.add_column('ping')
    t.add_column('state')
    t.add_column('in_error')
    t.add_column('included')
    t.add_row(status.name, format_bool(status.ping), status.state, format_bool(status.in_error), format_bool(status.included))
    if False:
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
            if i==0 and how_many == 1:
                t.add_row(first_one+status.name, format_bool(status.ping), status.state, format_bool(status.in_error), format_bool(status.included))
            elif i==0:
                t.add_row(first_many+status.name, format_bool(status.ping), status.state, format_bool(status.in_error), format_bool(status.included))
            elif i == how_many-1:
                t.add_row(last+status.name, format_bool(status.ping), status.state, format_bool(status.in_error), format_bool(status.included))
            else:
                t.add_row(next+status.name, format_bool(status.ping), status.state, format_bool(status.in_error), format_bool(status.included))

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
def some_command(obj:ControllerContext, command, arguments) -> None:
    from druncschema.controller_pb2 import FSMCommand
    args = {}
    if len(arguments) % 2 != 0:
        raise RuntimeError('Arguments are pairs of key value!')

    for i in range(int(len(arguments)/2)):
        args[arguments[i]] = arguments[i+1]

    data = FSMCommand(
        command_name = command,
        arguments = args,
    )
    send_command(
        controller = obj.controller,
        token = obj.token,
        command = 'execute_fsm_command',
        data = data
    )



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

