import grpc

from google.protobuf import any_pb2
from google.rpc import code_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

from drunc.communication.controller_pb2 import Request, Response, BroadcastMessage, Level, Token, PlainText, BroadcastRequest
from drunc.communication.controller_pb2_grpc import ControllerServicer, BroadcastStub
from drunc.utils.utils import now_str, setup_fancy_logging
import drunc.controller.exceptions as ctler_excpt

import threading
import time
import threading
from typing import Optional

class ControllerActor:
    def __init__(self, token:Optional[Token]=None):
        self._log = setup_fancy_logging("ControllerActor")
        self._token = Token()
        self._lock = threading.Lock()

    def get_token(self) -> Token:
        return self._token

    def get_user_name(self) -> str:
        return self._token.user_name

    def _update_actor(self, token:Optional[Token]=Token()) -> None:
        self._lock.acquire()
        self._token.CopyFrom(token)
        self._lock.release()

    def _compare_token(self, token1, token2):
        return token1.user_name == token2.user_name and token1.token == token2.token #!! come on protobuf, you can compare messages

    def surrender_control(self, token) -> None:
        if self._compare_token(self._token, token):
            self._update_actor(Token())
            return
        raise ctler_excpt.CannotSurrenderControl(f'Token {token} cannot release control of {self._token}')

    def take_control(self, token) -> None:
        if not self._compare_token(self._token, Token()):
            raise ctler_excpt.OtherUserAlreadyInControl(f'Actor {self._token.user_name} is already in control')

        self._update_actor(token)

class Controller(ControllerServicer):
    def __init__(self, name:str, configuration:str):
        self.log = setup_fancy_logging("Controller")
        super(Controller, self).__init__()
        self.name = name
        self.configuration_loc = configuration

        from drunc.controller.broadcaster import Broadcaster
        self.broadcaster = Broadcaster()

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        self.authoriser = DummyAuthoriser()

        self.actor = ControllerActor(None)

    def stop(self):
        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = 'over_and_out',
                emitter = self.name
            )
        )

    def _unpack(self, data, format):
        if not data.Is(format.DESCRIPTOR):
            self.log.error(f'Cannot unpack {data} into {format}')
            raise ctler_excpt.MalformedMessage()
        req = format()
        data.Unpack(req)
        return req


    def _generic_user_command(self, request:Request, command:str, context):
        """
        A generic way to execute the controller commands from a user.
        1. Check if the command is authorised
        2. Broadcast that the command is being executed
        3. Execute the command
        4. Broadcast that the command has been executed successfully or not
        5. Return the result
        """
        self.log.info(f'Attempting to execute {command}')

        if not self.authoriser.is_authorised(request.token, command):
            context.abort_with_status(
                rpc_status.to_status(
                    status_pb2.Status(
                        code=code_pb2.PERMISSION_DENIED,
                        message='Unauthorised',
                        details=[],
                    )
                )
            )
            self.log.error(f'Unauthorised attempt to execute {command} from {request.token.user_name}')

        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'Attempting to execute {command}',
                emitter = self.name
            )
        )

        try:
            data = request.data if request.data else None
            self.log.debug(f'{command} data: {request.data}')
            result = getattr(self, command+"_impl")(data, request.token)

        except ctler_excpt.ControllerException as e:
            self.log.error(f'ControllerException when executing {command}: {e}')

            self.broadcaster.new_broadcast(
                BroadcastMessage(
                    level = Level.INFO,
                    payload = f'ControllerException when executing {command}: {e}',
                    emitter = self.name
                )
            )

            detail = any_pb2.Any()
            detail.Pack(PlainText(text = f'ControllerException when executing {command}: {e}'))

            self.log.error(f'Aborting {command}')

            context.abort_with_status(
                rpc_status.to_status(
                    status_pb2.Status(
                        code=code_pb2.INTERNAL,
                        message='Exception thrown while executing the command',
                        details=[detail],
                    )
                )
            )
        except Exception as e:
            self.broadcaster.new_broadcast(
                BroadcastMessage(
                    level = Level.INFO,
                    payload = f'Unhandled exception when executing {command}: {e}',
                    emitter = self.name
                )
            )
            raise e # let gRPC handle it


        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'Successfully executed {command}: {result}',
                emitter = self.name
            )
        )

        result_any = any_pb2.Any()
        result_any.Pack(result) # pack response to any
        response = Response(data = result_any)
        self.log.info(f'Successfully executed {command}, response: {response}')
        return response



    def add_to_broadcast_list(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_add_to_broadcast_list', context)

    def _add_to_broadcast_list_impl(self, data, _) -> PlainText:
        r = self._unpack(data, BroadcastRequest)
        self.log.info(f'Adding {r.broadcast_receiver_address} to broadcast list')
        if not self.broadcaster.add_listener(r.broadcast_receiver_address):
            raise ctler_excpt.ControllerException(f'Failed to add {r.broadcast_receiver_address} to broadcast list')
        return PlainText(text = f'Added {r.broadcast_receiver_address} to broadcast list')



    def remove_from_broadcast_list(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_remove_from_broadcast_list', context)

    def _remove_from_broadcast_list_impl(self, data, _) -> PlainText:
        r = self._unpack(data, BroadcastRequest)
        if not self.broadcaster.rm_listener(r.broadcast_receiver_address):
            raise ctler_excpt.ControllerException(f'Failed to remove {r.broadcast_receiver_address} from broadcast list')
        return PlainText(text = f'Removed {r.broadcast_receiver_address} to broadcast list')



    def take_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_take_control', context)

    def _take_control_impl(self, _, token) -> PlainText:
        self.actor.take_control(token)
        return PlainText(text = f'User {token.user_name} took control')



    def surrender_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_surrender_control', context)

    def _surrender_control_impl(self, _, token) -> PlainText:
        user = self.actor.get_user_name()
        self.actor.surrender_control(token)
        return PlainText(text = f'User {user} surrendered control')



    def who_is_in_charge(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_who_is_in_charge', context)

    def _who_is_in_charge_impl(self, *args) -> PlainText:
        user = self.actor.get_user_name()
        return PlainText(text = f'{user}')
