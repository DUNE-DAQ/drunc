import click
import click_shell
from drunc.controller.controller import Controller
from drunc.interface.stdout_broadcast_handler import StdoutBroadcastHandler
from drunc.communication.controller_pb2 import Command, Token, Request, Response, BroadcastRequest, PlainText
import drunc.controller.exceptions as ctler_excpt
from drunc.utils.grpc_utils import send_command

import grpc
import google.protobuf.any_pb2 as any_pb2
from grpc_status import rpc_status

from drunc.utils.utils import now_str, setup_fancy_logging

class ControllerContext:
    def __init__(self, status_receiver_port:str=None) -> None:
        self.log = setup_fancy_logging("Controller Shell")
        self.print_traceback = True
        self.controller = None

        import os
        user = os.getlogin()

        self.token = Token ( # fake token, but should be figured out from the environment/authoriser
            token = 'abc',
            user_name = user
        )

        self.broadcasted_to = False

        if status_receiver_port is None: return

        self.status_receiver = StdoutBroadcastHandler(
            port = status_receiver_port
        )

        from threading import Thread
        self.server_thread = Thread(target=self.status_receiver.serve, name=f'serve_thread')
        self.server_thread.start()


@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('controller-address', type=str)#, help='Which address the controller is running on')
@click.argument('this-port', type=int)#, help='Which port to use for receiving status')
@click.option('--just-watch', type=bool, default=False, is_flag=True, help='If one just doesn\'t want to take control of the controller')
@click.pass_context
def controller_shell(ctx, controller_address:str, this_port:int, just_watch:bool) -> None:
    ctx.obj = ControllerContext(this_port)

    import time
    while ctx.obj.status_receiver.ready is False:
        time.sleep(0.1)

    # first add the shell to the controller broadcast list
    from drunc.communication.controller_pb2_grpc import ControllerStub
    import grpc
    ctx.obj.log.info('Connecting to controller')

    channel = grpc.insecure_channel(controller_address)

    ctx.obj.controller = ControllerStub(channel)
    ctx.obj.log.info('Adding this shell to the broadcast list.')

    try:
        response = send_command(
            ctx.obj.controller,
            ctx.obj.token,
            'add_to_broadcast_list',
            BroadcastRequest(broadcast_receiver_address =  f'[::]:{this_port}')
        )
        # this command returns a response with a plain text message
        pt = PlainText()
        response.data.Unpack(pt)
        ctx.obj.log.info(pt)
        ctx.obj.broadcasted_to = True
    except Exception as e:
        ctx.obj.log.error('Could not add this shell to the broadcast list.')
        ctx.obj.log.error(e)
        ctx.obj.log.error('Exiting.')
        ctx.obj.status_receiver.stop()
        ctx.obj.server_thread.join()
        raise e


    def cleanup():
        # remove the shell from the controller broadcast list
        dead = False
        if ctx.obj.broadcasted_to:
            ctx.obj.log.debug('Removing this shell from the broadcast list.')
            try:
                response = send_command(
                    ctx.obj.controller,
                    ctx.obj.token,
                    'remove_from_broadcast_list',
                    BroadcastRequest(broadcast_receiver_address =  f'[::]:{this_port}'),
                    rethrow=True
                )
                ctx.obj.log.debug('Removed this shell from the broadcast list.')
            except grpc.RpcError as e:
                dead = grpc.StatusCode.UNAVAILABLE == e.code()
            except Exception as e:
                ctx.obj.log.error('Could not remove this shell from the broadcast list.')
                ctx.obj.log.error(e)

        if dead:
            ctx.obj.log.error('Controller is dead. Exiting.')
            ctx.obj.status_receiver.stop()
            ctx.obj.server_thread.join()
            return

        from drunc.utils.grpc_utils import unpack_any
        try:
            response = send_command(
                ctx.obj.controller,
                ctx.obj.token,
                'who_is_in_charge',
                None,
                rethrow=True
            )
            pt = unpack_any(response.data, PlainText)
        except Exception as e:
            ctx.obj.log.error('Could not understand who is in charge from the controller.')
            ctx.obj.log.error(e)
            pt = 'no_one'


        if pt.text == ctx.obj.token.user_name:
            ctx.obj.log.info('You are in control. Surrendering control.')
            try:
                response = send_command(
                    ctx.obj.controller,
                    ctx.obj.token,
                    'surrender_control',
                    None,
                    rethrow=True
                )
            except Exception as e:
                ctx.obj.log.error('Could not surrender control.')
                ctx.obj.log.error(e)
            ctx.obj.log.info('Control surrendered.')

        ctx.obj.status_receiver.stop()
        ctx.obj.server_thread.join()

    ctx.call_on_close(cleanup)

    # If we are just interested in watching the controller, then we are done here
    if just_watch:
        return

    # then take control of the controller
    ctx.obj.log.info(f'Taking control of the controller as {ctx.obj.token}')
    try:
        response = send_command(
            ctx.obj.controller,
            ctx.obj.token,
            'take_control',
            None
        )
    except Exception as e:
        ctx.obj.log.error('You NOT are in control.')
        raise e
    ctx.obj.log.info('You are in control.')


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


@controller_shell.command('who-is-in-charge')
@click.pass_obj
def who_is_in_charge(obj:ControllerContext) -> None:
    send_command(
        controller = obj.controller,
        token = obj.token,
        command = 'who_is_in_charge',
        data = None
    )


@controller_shell.command('some-command')
@click.pass_obj
def some_command(obj:ControllerContext) -> None:
    raise NotImplementedError('This is just an example command, so it is not implemented.')

