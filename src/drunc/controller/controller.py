import grpc
from drunc.communication.controller_pb2 import BroadcastRequest, GenericResponse, BroadcastMessage, Level, Token, ResponseCode
from drunc.communication.controller_pb2_grpc import ControllerServicer, BroadcastStub
from drunc.utils.utils import now_str
from typing import Optional
import threading
from drunc.utils.utils import setup_fancy_logging
import time


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

    def stop(self):
        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'over_and_out',
                emitter = self.name
            )
        )
        #self.broadcaster.stop()

    def generic_user_command(self, request, command):
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
            return GenericResponse(
                response_code = ResponseCode.UNAUTHORIZED,
                response_text = 'Unauthorized'
            )

        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'Attempting to execute {command}',
                emitter = self.name
            )
        )

        try:
            result = getattr(self, command+"_impl")(request)
        except Exception as e: # Maybe better to let the exception propagate? gRPC is pretty good for this
            self.broadcaster.new_broadcast(
                BroadcastMessage(
                    level = Level.INFO,
                    payload = f'Failed to execute {command}: {e}',
                    emitter = self.name
                )
            )
            return GenericResponse(
                response_code = ResponseCode.FAILED,
                response_text = f'Failed to execute {command}: {e}'
            )

        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'Successfully executed {command}',
                emitter = self.name
            )
        )
        self.log.info(f'Successfully executed {command}')
        return GenericResponse(
            response_code = ResponseCode.DONE,
            response_text = f'Successfully executed {command}',
        )

    def add_to_broadcast_list(self, br:BroadcastRequest, context) -> GenericResponse:
        return self.generic_user_command(br, 'add_to_broadcast_list')

    def add_to_broadcast_list_impl(self, br:BroadcastRequest):
        if not self.broadcaster.add_listener(br.address_status_endpoint):
            raise Exception(f'Failed to add {br.address_status_endpoint} to broadcast list')

    def remove_from_broadcast_list(self, br:BroadcastRequest, context) -> GenericResponse:
        return self.generic_user_command(br, 'remove_from_broadcast_list')

    def remove_from_broadcast_list_impl(self, br:BroadcastRequest):
        if not self.broadcaster.rm_listener(br.address_status_endpoint):
            raise Exception(f'Failed to remove {br.address_status_endpoint} from broadcast list')

    def take_control(self, br:Token, context) -> GenericResponse:
        return self.generic_user_command(br, 'take_control')

    def take_control_impl(self, br:Token):
        self.broadcaster.rm_listener(br.address_status_endpoint)

    def surrender_control(self, br:Token, context) -> GenericResponse:
        return self.generic_user_command(br, 'surrender_control')

    def surrender_control_impl(self, br:Token):
        self.broadcaster.rm_listener(br.address_status_endpoint)