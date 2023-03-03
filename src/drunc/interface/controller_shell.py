import click
import click_shell
from drunc.controller.controller import Controller
from drunc.interface.stdout_broadcast_handler import StdoutBroadcastHandler
from drunc.communication.controller_pb2 import Command, Token, BroadcastRequest, GenericResponse, ResponseCode
import grpc
from drunc.utils.utils import now_str, setup_fancy_logging

class ControllerContext:
    def __init__(self, status_receiver_port:str=None) -> None:
        self.log = setup_fancy_logging("Controller Shell")
        self.print_traceback = True
        self.controller = None
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
@click.pass_obj
@click.pass_context
def controller_shell(ctx, obj:ControllerContext, controller_address:str, this_port:int, just_watch:bool) -> None:
    obj = ControllerContext(this_port)
    import time
    while obj.status_receiver.ready is False:
        time.sleep(0.1)

    # first add the shell to the controller broadcast list
    from drunc.communication.controller_pb2_grpc import ControllerStub
    import grpc
    obj.log.info('Connecting to controller')

    channel = grpc.insecure_channel(controller_address)

    obj.controller = ControllerStub(channel)
    obj.log.info('Adding this shell to the broadcast list.')

    response = obj.controller.add_to_broadcast_list(
        BroadcastRequest(
            token = Token (text = 'abc'), # fake token, but should be figured out from the user
            address_status_endpoint = f'[::]:{this_port}'
        )
    )

    added_to_broadcast = response.response_code == ResponseCode.DONE
    if not added_to_broadcast:
        obj.log.error('Could not add this shell to the broadcast list.')
        obj.log.error(response.response_text)
        obj.log.error('Exiting.')
        obj.status_receiver.stop()
        obj.server_thread.join()
        exit(1)
    obj.log.info(response.response_text)

    def cleanup():
        # remove the shell from the controller broadcast list
        if added_to_broadcast:
            obj.log.debug('Removing this shell from the broadcast list.')
            response = obj.controller.remove_from_broadcast_list(
                BroadcastRequest(
                    token = Token (text = 'abc'), # fake token, but should be figured out from the user
                    address_status_endpoint = f'[::]:{this_port}'
                )
            )
            if not response.response_code == ResponseCode.DONE:
                obj.log.error('Could not remove this shell from the broadcast list.')
                obj.log.error(response.response_text)
            obj.log.debug('Removed this shell from the broadcast list.')

        obj.status_receiver.stop()
        obj.server_thread.join()

    ctx.call_on_close(cleanup)

    # If we are just interested in watching the controller, then we are done here
    if just_watch:
        return

    # then take control of the controller
    # generic_response = obj.controller.take_control(
    #     BroadcastRequest(
    #         token = Token (
    #             text = 'abc'
    #         ),
    #         address_status_endpoint = this_address
    #     )
    # )



@controller_shell.command('some-command')
@click.pass_obj
def some_command(obj:ControllerContext) -> None:
    import json
    import random
    result = obj.rc.execute_command(
        command = Command (
            command_name = 'some-command',
            command_data = json.dumps({'wait_for': random.random()*2.}),
            controlled_name = "",
            controller_name = obj.rc.name,
            datetime = now_str()
        )
    )
    print(result)
