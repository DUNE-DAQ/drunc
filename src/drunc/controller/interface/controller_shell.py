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

        if ctler_conf is None:
            return

        import os
        user = os.getlogin()

        from druncschema.token_pb2 import Token
        self.token = Token ( # fake token, but should be figured out from the environment/authoriser
            token = f'{user}-token',
            user_name = user
        )

        self.ctler_conf_data = {}
        with open(ctler_conf) as f:
            import json
            self.ctler_conf_data = json.loads(f.read())


        self.status_receiver = None
        self.took_control = False

    def start_listening(self, topic):
        from drunc.broadcast.client.kafka_stdout_broadcast_handler import KafkaStdoutBroadcastHandler
        from druncschema.broadcast_pb2 import BroadcastMessage

        self.status_receiver = KafkaStdoutBroadcastHandler(
            conf = self.ctler_conf_data['broadcaster'],
            topic = topic,
            message_format = BroadcastMessage,
        )


    def terminate(self):
        self.status_receiver.stop()

    def print(self, text):
        self._console.print(text)

    def rule(self, text):
        self._console.rule(text)


@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('controller-address', type=str)#, help='Which address the controller is running on')
# @click.argument('this-port', type=int)#, help='Which port to use for receiving status')
# @click.option('--just-watch', type=bool, default=False, is_flag=True, help='If one just doesn\'t want to take control of the controller')
@click.argument('conf', type=click.Path(exists=True))
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_context
def controller_shell(ctx, controller_address:str, conf, log_level:str) -> None:#, this_port:int, just_watch:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj = ControllerContext(conf)

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
    ctx.obj.start_listening(
        f'{desc.name}.{desc.session}'
    )

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
        ctx.obj.log.error('You NOT are in control.')
        ctx.obj.took_control = False
        #raise e
        return

    ctx.obj.log.info('You are in control.')


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


@controller_shell.command('some-command')
@click.pass_obj
def some_command(obj:ControllerContext) -> None:
    raise NotImplementedError('This is just an example command, so it is not implemented.')

