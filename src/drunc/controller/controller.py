import grpc

from google.rpc import code_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

from drunc.controller.child_node import ChildNode
from drunc.controller.daq_app_child import DAQAppChild
from drunc.controller.controller_child import ControllerChild
from druncschema.request_response_pb2 import Request, Response
from druncschema.token_pb2 import Token
from druncschema.generic_pb2 import PlainText, PlainTextVector
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.controller_pb2_grpc import ControllerServicer
from drunc.broadcast.server.broadcast_sender import BroadcastSender
import drunc.controller.exceptions as ctler_excpt
from drunc.utils.grpc_utils import pack_to_any
from threading import Lock, Thread
import time
from typing import Optional, Dict, List


class ControllerActor:
    def __init__(self, token:Optional[Token]=None):
        from logging import getLogger
        self._log = getLogger("ControllerActor")
        self._token = Token()
        self._lock = Lock()

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



class Controller(ControllerServicer, BroadcastSender):
    def __init__(self, name:str, session:str, configuration:str):
        super(Controller, self).__init__()

        from logging import getLogger
        self._log = getLogger(name)
        self.name = name
        self.session = session
        self.configuration_loc = configuration

        from drunc.controller.configuration import ControllerConfiguration
        self.configuration = ControllerConfiguration(self.configuration_loc)
        self.children_nodes = [] # type: List[ChildNode]

        BroadcastSender.__init__(self, self.configuration.get_broadcaster_configuration(), self._log)

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import SystemType
        self.authoriser = DummyAuthoriser({}, SystemType.CONTROLLER)

        self.actor = ControllerActor(None)

        self.controller_token = Token(
            user_name = f'{self.name}_controller',
            token = 'broadcast_token' # massive hack here, controller should use user token to execute command, and have a "broadcasted to" token
        )

        for child_controller_cfg in self.configuration.children_controllers:
            self.children_nodes.append(ControllerChild(child_controller_cfg, self.configuration.broadcast_receiving_port, self.controller_token))

        for app_cfg in self.configuration.applications:
            self.children_nodes.append(DAQAppChild(app_cfg))

        # do this at the end, otherwise we need to self.stop() if an exception is raised
        self.broadcast(
            message = 'ready',
            btype = BroadcastType.SERVER_READY
        )

    def terminate(self):
        self.broadcast(
            btype = BroadcastType.SERVER_SHUTDOWN,
            message = 'over_and_out',
        )

        self._log.info('Stopping children')
        for child in self.children_nodes:
            self._log.debug(f'Stopping {child.name}')
            child.terminate()



    def _propagate_to_list(self, request:Request, command:str, context, node_to_execute:Dict[ChildNode, str]):

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_START,
            message = f'Propagating {command} to children',
        )

        def propagate_to_child(child, command, data, token, location_override):

            self.broadcast(
                btype = BroadcastType.CHILD_COMMAND_EXECUTION_START,
                message = f'Propagating {command} to children ({child.node_type.name})',
            )

            try:
                child.propagate_command(command, data, token, location)
                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_SUCCESS,
                    message = f'Propagating {command} to children ({child.node_type.name})',
                )
            except:
                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_FAILED,
                    message = f'Failed to propagate {command} to {child.name} ({child.node_type.name})',
                )

        threads = []
        for child, location in node_to_execute.items():
            self._log.debug(f'Propagating to {child.name}')
            t = Thread(target=propagate_to_child, args=(child, command, request.data, request.token, location))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_END,
            message = f'Propagated {command} to children',
        )

    def _should_execute_on_self(self, node_path) -> bool:
        if node_path == []:
            return True

        for node in node_path:
            if node == [self.name]:
                return True
        return False

    def _generic_user_command(self, request:Request, command:str, context, propagate=False):
        """
        A generic way to execute the controller commands from a user.
        1. Check if the command is authorised
        2. Broadcast that the command is being executed
        3. Execute the command on children controller, app, and self
        4. Broadcast that the command has been executed successfully or not
        5. Return the result
        """
        self.broadcast(
            btype = BroadcastType.COMMAND_RECEIVED,
            message = f'{request.token.user_name} attempting to execute {command}'
        )

        if not self.authoriser.is_authorised(request.token, command):
            self.broadcast(
                btype = BroadcastType.TEXT_MESSAGE, # make this an real type rather than just text
                message = f'{request.token.user_name} is not authorised to execute {command}',
            )
            context.abort_with_status(
                rpc_status.to_status(
                    status_pb2.Status(
                        code=code_pb2.PERMISSION_DENIED,
                        message='Unauthorised',
                        details=[],
                    )
                )
            )


        data = request.data if request.data else None
        self._log.debug(f'{command} data: {request.data}')

        if propagate:
            self.propagate_to_list(request, command, self.children_nodes)

        try:
            token = Token()
            token.CopyFrom(request.token)
            self.broadcast(
                btype = BroadcastType.COMMAND_EXECUTION_START,
                message = f'Executing {command} (upon request from {request.token.user_name})',
            )
            result = getattr(self, command+"_impl")(data, token)


        except ctler_excpt.ControllerException as e:
            self.broadcast(
                btype = BroadcastType.EXCEPTION_RAISED,
                message = f'ControllerException when executing {command}: {e}'
            )

            detail = pack_to_any(PlainText(text = f'ControllerException when executing {command}: {e}'))


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
            self.broadcast(
                btype = BroadcastType.UNHANDLED_EXCEPTION_RAISED,
                message = f'Unhandled exception when executing {command}: {e}'
            )
            raise e # let gRPC handle it

        result_any = pack_to_any(result)
        response = Response(data = result_any)

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS,
            message = f'Succesfully executed {command}'
        )

        return response



    def ls(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_ls', context, propagate=False)

    def _ls_impl(self, _, dummy) -> PlainTextVector:
        nodes = [node.name for node in self.children_nodes]
        return PlainTextVector(
            text = nodes
        )

    def describe(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_describe', context, propagate=False)

    def _describe_impl(self, _, dummy):
        from druncschema.request_response_pb2 import Description
        return Description(
            name = self.name,
            session = self.session,
            # ... list of commands, etc...
        )


    def take_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_take_control', context)

    def _take_control_impl(self, _, token) -> PlainText:
        self.actor.take_control(token)
        return PlainText(text = f'{token.user_name} took control')



    def surrender_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_surrender_control', context)

    def _surrender_control_impl(self, _, token) -> PlainText:
        user = self.actor.get_user_name()
        self.actor.surrender_control(token)
        return PlainText(text = f'{user} surrendered control')



    def who_is_in_charge(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_who_is_in_charge', context)

    def _who_is_in_charge_impl(self, *args) -> PlainText:
        user = self.actor.get_user_name()
        return PlainText(text = user)
