import grpc

from google.protobuf import any_pb2
from google.rpc import code_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

from drunc.communication.child_node import ChildNode
from drunc.communication.daq_app_child import DAQAppChild
from drunc.communication.controller_child import ControllerChild
from drunc.communication.controller_pb2 import Request, Response, BroadcastMessage, Level, Token, PlainText, BroadcastRequest, StringStringMap, Location, LocationList
from drunc.communication.controller_pb2_grpc import ControllerServicer, BroadcastStub
from drunc.utils.utils import get_logger
import drunc.controller.exceptions as ctler_excpt
from drunc.utils.grpc_utils import unpack_any
from threading import Lock, Thread
import time
from typing import Optional, Dict, List


class ControllerActor:
    def __init__(self, token:Optional[Token]=None):
        self._log = get_logger("ControllerActor")
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



class Controller(ControllerServicer):
    def __init__(self, name:str, configuration:str):
        super(Controller, self).__init__()

        self.log = get_logger("Controller")
        self.name = name
        self.configuration_loc = configuration

        from drunc.controller.configuration import ControllerConfiguration
        self.configuration = ControllerConfiguration(self.configuration_loc)
        self.children_nodes = [] # type: List[ChildNode]

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        self.authoriser = DummyAuthoriser(configuration['authoriser'])

        self.actor = ControllerActor(None)

        self.controller_token = Token(
            user_name = f'{self.name}_controller',
            token = 'broadcast_token' # massive hack here, controller should use user token to execute command, and have a "broadcasted to" token
        )

        for child_controller_cfg in self.configuration.children_controllers:
            self.children_nodes.append(ControllerChild(child_controller_cfg, self.configuration.broadcast_receiving_port, self.controller_token))

        for app_cfg in self.configuration.applications:
            self.children_nodes.append(DAQAppChild(app_cfg))

        from drunc.interface.stdout_broadcast_handler import StdoutBroadcastHandler
        self.broadcast_handler = StdoutBroadcastHandler(self.configuration.broadcast_receiving_port)
        self.broadcast_server_thread = Thread(target=self.broadcast_handler.serve, name=f'broadcast_serve_thread')
        self.broadcast_server_thread.start()

        # do this at the end, otherwise we need to self.stop() if an exception is raised
        from drunc.controller.broadcaster import Broadcaster
        self.broadcaster = Broadcaster()

        self.log.info('Controller initialised')

    def stop(self):
        self.log.info(f'Stopping controller {self.name}')

        if self.broadcaster:
            self.log.info('Stopping broadcaster')
            self.broadcaster.new_broadcast(
                BroadcastMessage(
                    level = Level.INFO,
                    payload = 'over_and_out',
                    emitter = self.name
                )
            )

        if self.broadcast_server_thread:
            self.log.info('Stopping broadcast receiver thread')
            self.broadcast_handler.stop()
            self.broadcast_server_thread.join()

        self.log.info('Stopping children')
        for child in self.children_nodes:
            self.log.debug(f'Stopping {child.name}')
            child.stop()



    def _propagate_to_list(self, request:Request, command:str, context, node_to_execute:Dict[ChildNode, str]):

        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'Propagating {command} to children',
                emitter = self.name
            )
        )

        def propagate_to_child(child, command, data, token, location_override):

            self.broadcaster.new_broadcast(
                BroadcastMessage(
                    level = Level.DEBUG,
                    payload = f'Propagating {command} to children ({child.node_type.name})',
                    emitter = self.name
                )
            )

            try:
                child.propagate_command(command, data, token, location)
                self.broadcaster.new_broadcast(
                    BroadcastMessage(
                        level = Level.DEBUG,
                        payload = f'Propagating {command} to children ({child.node_type.name})',
                        emitter = self.name
                    )
                )
            except:
                self.log.error(f'Failed to propagate {command} to {child.name} ({child.node_type.name})')
                self.broadcaster.new_broadcast(
                    BroadcastMessage(
                        level = Level.ERROR,
                        payload = f'Failed to propagate {command} to {child.name} ({child.node_type.name})',
                        emitter = self.name
                    )
                )

        threads = []
        for child, location in node_to_execute.items():
            self.log.debug(f'Propagating to {child.name}')
            t = Thread(target=propagate_to_child, args=(child, command, request.data, request.token, location))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

        self.broadcaster.new_broadcast(
            BroadcastMessage(
                level = Level.INFO,
                payload = f'Propagated {command} to children',
                emitter = self.name
            )
        )

    def _should_execute_on_self(self, node_path) -> bool:
        if node_path == []:
            return True

        for node in node_path:
            if node == [self.name]:
                return True
        return False

    def _resolve(self, paths) -> dict[ChildNode, Location]:

        ret = {}
        from drunc.utils.utils import regex_match
        for loc in paths:

            if loc.nodes[0] != self.name:
                continue

            elif loc.nodes == [self.name] and loc.recursive:
                for node in self.nodes:
                    if node in ret: # TODO my own error here please
                        raise RuntimeError(f'Mutliple command paths for the same node! \'{node.name}\' should propagate to \'{ret[node]}\' and to \'{loc}\'')
                    ret[node] = Location(
                        nodes = [node.name],
                        recursive = True
                    )

            elif loc.nodes == [self.name] and not loc.recursive:
                continue

            else:
                for cn in self.children_nodes:
                    if regex_match(cn.name, node):
                        if node in ret: # TODO my own error here please
                            raise RuntimeError(f'Mutliple command paths for the same node! \'{node.name}\' should propagate to \'{ret[node]}\' and to \'{loc}\'')
                        ret[cn] = Location(
                            nodes = [cn.name] + loc.nodes[2:] if len(loc.nodes)>2 else [],
                            recursive = loc.recursive
                        )

        return ret


    def _generic_user_command(self, request:Request, command:str, context):
        """
        A generic way to execute the controller commands from a user.
        1. Check if the command is authorised
        2. Broadcast that the command is being executed
        3. Execute the command on children controller, app, and self
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
                payload = f'{request.token.user_name} is attempting to execute {command}',
                emitter = self.name
            )
        )
        data = request.data if request.data else None
        self.log.debug(f'{command} data: {request.data}')

        node_to_execute = self._resolve(request.locations)

        if node_to_execute:
            self.propagate_to_list(request, command, node_to_execute) # TODO this function needs to bundle the results

        if self._should_execute_on_self(request.locations):
            try:
                token = Token()
                token.CopyFrom(request.token)
                self.log.info(f'{token} executing {command}')
                result = getattr(self, command+"_impl")(data, token)

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



    def ls(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_ls', context)

    def _ls_impl(self, _, dummy) -> LocationList:
        self.log.info(f'Listing children nodes')
        locs = [
            Location(
                path = [self.name],
                recursive = False
            )
        ]+[
            Location(
                path = [self.name, node.name],
                recursive = False
            ) for node in self.children_nodes
        ]
        print(self.children_nodes)

        self.log.info(f'All the paths accessible: {locs}')
        return LocationList(
            locations = locs

        )


    def add_to_broadcast_list(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_add_to_broadcast_list', context)

    def _add_to_broadcast_list_impl(self, data, _) -> PlainText:
        r = unpack_any(data, BroadcastRequest)
        self.log.info(f'Adding {r.broadcast_receiver_address} to broadcast list')
        if not self.broadcaster.add_listener(r.broadcast_receiver_address):
            raise ctler_excpt.ControllerException(f'Failed to add {r.broadcast_receiver_address} to broadcast list')
        return PlainText(text = f'Added {r.broadcast_receiver_address} to broadcast list')



    def remove_from_broadcast_list(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_remove_from_broadcast_list', context)

    def _remove_from_broadcast_list_impl(self, data, _) -> PlainText:
        r = unpack_any(data, BroadcastRequest)
        if not self.broadcaster.rm_listener(r.broadcast_receiver_address):
            raise ctler_excpt.ControllerException(f'Failed to remove {r.broadcast_receiver_address} from broadcast list')
        return PlainText(text = f'Removed {r.broadcast_receiver_address} to broadcast list')



    def get_broadcast_list(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_get_broadcast_list', context)

    def _get_broadcast_list_impl(self, dummy, _) -> PlainText:
        ret = StringStringMap()
        listeners = self.broadcaster.get_listeners()
        for k, v in listeners.items():
            ret[k] = v
        return


    def take_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, '_take_control', context)

    def _take_control_impl(self, _, token) -> PlainText:
        self.actor.take_control(token)
        self.log.info(f'User {token.user_name} took control')
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
