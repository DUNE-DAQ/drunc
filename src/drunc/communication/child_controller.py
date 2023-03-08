import asyncio
import grpc
from typing import Optional
# from drunc.communication.controller_pb2 import Command, CommandResponse
# from drunc.communication.controller_pb2_grpc import ControllerStub
from drunc.communication.controller_pb2 import Request, Response, BroadcastMessage, Level, Token, PlainText, BroadcastRequest

class ChildControllerChannel():
    def __init__(self, config:dict, broadcasting_port:int, controller_token:Token):
        self.log = setup_fancy_logging("Controller")
        self.token = controller_token
        self.cmd_address = config['cmd_address']
        self.command_channel = grpc.insecure_channel(self.cmd_address)
        self.controller = ControllerStub(channel)
        self.broadcasting_port = broadcasting_port

        # setup the broadcasting
        try:
            response = send_command(
                self.controller,
                self.controller_token,
                'add_to_broadcast_list',
                BroadcastRequest(broadcast_receiver_address =  f'[::]:{broadcasting_port}')
            )
            # this command returns a response with a plain text message
            pt = PlainText()
            response.data.Unpack(pt)
            self.log.info(pt)
            self.broadcasted_to = True
        except Exception as e:
            self.log.error(f'Could not add this controller to the broadcast list of {self.cmd_address}.')
            self.log.error(e)
            self.broadcasted_to = False

    def propagate_command(self, command, data, token):
        request = Request()
        request.token.CopyFrom(token)
        request.data.CopyFrom(data)
        return getattr(self, command)(request)


    def close(self):
        print('Closing the connection')
        self.command_channel.close()
        if self.broadcasted_to:
            self.log.debug(f'Removing this {self.controller_token.user_name} from {self.cmd_address}\'s broadcast list.')
            dead = False
            try:
                response = send_command(
                    self.controller,
                    self.token,
                    'remove_from_broadcast_list',
                    BroadcastRequest(broadcast_receiver_address =  f'[::]:{self.broadcasting_port}'),
                    rethrow=True
                )
                self.log.debug('Removed this shell from the broadcast list.')

            except grpc.RpcError as e:
                dead = grpc.StatusCode.UNAVAILABLE == e.code()
            except Exception as e:
                self.log.error(f'Could not remove {self.controller_token.user_name} from {self.cmd_address}\'s broadcast list.')
                self.log.error(e)

        if dead:
            self.log.error('Controller is dead. Exiting.')
            self.status_receiver.stop()
            self.server_thread.join()
            return
