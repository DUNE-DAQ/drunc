from druncschema.request_response_pb2 import Request, Response, ResponseFlag
from druncschema.token_pb2 import Token
from druncschema.generic_pb2 import PlainText, PlainTextVector
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.controller_pb2_grpc import ControllerServicer
from druncschema.controller_pb2 import Status, ChildrenStatus
from druncschema.controller_pb2 import FSMCommand, FSMCommandResponse, FSMResponseFlag

from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.stateful_node import StatefulNode
from drunc.broadcast.server.broadcast_sender import BroadcastSender
import drunc.controller.exceptions as ctler_excpt
from drunc.utils.grpc_utils import pack_to_any
from typing import Optional, List
from drunc.broadcast.server.decorators import broadcasted
from drunc.utils.grpc_utils import unpack_request_data_to, pack_response
from drunc.authoriser.decorators import authentified_and_authorised
from druncschema.authoriser_pb2 import ActionType, SystemType
from drunc.controller.decorators import in_control
from drunc.exceptions import DruncException

import signal

class ControllerActor:
    def __init__(self, token:Optional[Token]=None):
        from logging import getLogger
        self.logger = getLogger("ControllerActor")

        self._token = Token(
            token="",
            user_name="",
        )

        if token is not None:
            self._token.CopyFrom(token)

        from threading import Lock
        self._lock = Lock()

    def get_token(self) -> Token:
        return self._token

    def get_user_name(self) -> str:
        return self._token.user_name

    def _update_actor(self, token:Optional[Token]=Token()) -> None:
        self._lock.acquire()
        self._token.CopyFrom(token)
        self._lock.release()

    def compare_token(self, token1, token2):
        self._lock.acquire()
        result = token1.user_name == token2.user_name and token1.token == token2.token #!! come on protobuf, you can compare messages
        self._lock.release()
        return result

    def token_is_current_actor(self, token):
        return self.compare_token(token, self._token)

    def surrender_control(self, token) -> None:
        if self.compare_token(self._token, token):
            self._update_actor(Token())
            return
        raise ctler_excpt.CannotSurrenderControl(f'Token {token} cannot release control of {self._token}')

    def take_control(self, token) -> None:
        # if not self.compare_token(self._token, token):
        #     raise ctler_excpt.OtherUserAlreadyInControl(f'Actor {self._token.user_name} is already in control')
        self._update_actor(token)
        return 0


class Controller(ControllerServicer):

    children_nodes = [] # type: List[ChildNode]

    def __init__(self, configuration, name:str, session:str, token:Token):
        super().__init__()
        self.name = name
        self.session = session
        self.broadcast_service = None

        from logging import getLogger
        self.logger = getLogger('Controller')

        self.configuration = configuration

        from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
        bsch = BroadcastSenderConfHandler(
            data = self.configuration.data.controller.broadcaster,
        )

        self.broadcast_service = BroadcastSender(
            name = name,
            session = session,
            configuration = bsch,
        )


        from drunc.fsm.configuration import FSMConfHandler
        fsmch = FSMConfHandler(
            data = self.configuration.data.controller.fsm,
        )

        self.stateful_node = StatefulNode(
            fsm_configuration = fsmch,
            broadcaster = self.broadcast_service
        )

        from drunc.authoriser.configuration import DummyAuthoriserConfHandler
        dach = DummyAuthoriserConfHandler(
            data = self.configuration.authoriser,
        )

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import SystemType
        self.authoriser = DummyAuthoriser(
            dach,
            SystemType.CONTROLLER
        )

        self.actor = ControllerActor(token)

        self.connectivity_service = None
        self.connectivity_service_thread = None
        self.uri = ''
        if self.configuration.session.connectivity_service:
            import os
            connection_server = self.configuration.session.connectivity_service.host
            connection_port   = self.configuration.session.connectivity_service.service.port
            self.logger.info(f'Connectivity server {connection_server}:{connection_port} is enabled')

            from drunc.connectivity_service.client import ConnectivityServiceClient
            self.connectivity_service = ConnectivityServiceClient(
                    session = self.session,
                    address = f'{connection_server}:{connection_port}',
                )

        self.children_nodes = self.configuration.get_children(
            init_token = self.actor.get_token(),
            connectivity_service = self.connectivity_service
        )

        for child in self.children_nodes:
            self.logger.info(child)
            child.propagate_command('take_control', None, self.actor.get_token())

        from druncschema.request_response_pb2 import CommandDescription
        # TODO, probably need to think of a better way to do this?
        # Maybe I should "bind" the commands to their methods, and have something looping over this list to generate the gRPC functions
        # Not particularly pretty...
        self.commands = [
            CommandDescription(
                name = 'describe',
                data_type = ['None'],
                help = 'Describe self (return a list of commands, the type of endpoint, the name and session).',
                return_type = 'request_response_pb2.Description'
            ),

            CommandDescription(
                name = 'get_children_status',
                data_type = ['generic_pb2.PlainText','None'],
                help = 'Get the status of all the children. Only get the status from the child if provided in the request.',
                return_type = 'controller_pb2.ChildrenStatus'
            ),

            CommandDescription(
                name = 'get_status',
                data_type = ['None'],
                help = 'Get the status of self',
                return_type = 'controller_pb2.Status'
            ),

            CommandDescription(
                name = 'ls',
                data_type = ['None'],
                help = 'List the children',
                return_type = 'generic_pb2.PlainTextVector'
            ),

            CommandDescription(
                name = 'describe_fsm',
                data_type = ['generic_pb2.PlainText', 'None'],
                help = '''Return a description of the FSM transitions:
if a transition name is provided in its input, return that transition description;
if a state is provided, return the transitions accessible from that state;
if "all-transitions" is provided, return all the transitions;
if nothing (None) is provided, return the transitions accessible from the current state.''',
                return_type = 'request_response_pb2.Description'
            ),

            CommandDescription(
                name = 'execute_fsm_command',
                data_type = ['controller_pb2.FSMCommand'],
                help = 'Execute an FSM command',
                return_type = 'controller_pb2.FSMCommandResponse'
            ),

            CommandDescription(
                name = 'include',
                data_type = ['None'],
                help = 'Include self in the current session, if a children is provided, include it and its eventual children',
                return_type = 'controller_pb2.FSMCommandResponse'
            ),

            CommandDescription(
                name = 'exclude',
                data_type = ['None'],
                help = 'Exclude self in the current session, if a children is provided, exclude it and its eventual children',
                return_type = 'controller_pb2.FSMCommandResponse'
            ),

            CommandDescription(
                name = 'take_control',
                data_type = ['None'],
                help = 'Take control of self and children',
                return_type = 'generic_pb2.PlainText'
            ),

            CommandDescription(
                name = 'surrender_control',
                data_type = ['None'],
                help = 'Surrender control of self and children',
                return_type = 'generic_pb2.PlainText'
            ),

            CommandDescription(
                name = 'who_is_in_charge',
                data_type = ['None'],
                help = 'Get who is in control of self',
                return_type = 'generic_pb2.PlainText'
            ),
        ]

        # do this at the end, otherwise we need to self.terminate() if an exception is raised
        self.broadcast(
            message = 'ready',
            btype = BroadcastType.SERVER_READY
        )


    '''
    A couple of simple pass-through functions to the broadcasting service
    '''
    def broadcast(self, *args, **kwargs):
        return self.broadcast_service.broadcast(*args, **kwargs)

    def can_broadcast(self, *args, **kwargs):
        if self.broadcast_service:
            return self.broadcast_service.can_broadcast(*args, **kwargs)
        return False

    def describe_broadcast(self, *args, **kwargs):
        return self.broadcast_service.describe_broadcast(*args, **kwargs)

    def interrupt_with_exception(self, *args, **kwargs):
        return self.broadcast_service._interrupt_with_exception(*args, **kwargs)

    def async_interrupt_with_exception(self, *args, **kwargs):
        return self.broadcast_service._async_interrupt_with_exception(*args, **kwargs)


    def construct_error_node_response(self, command_name:str, token:Token, cause:FSMResponseFlag) -> Response:
        fsm_result = FSMCommandResponse(
            flag = cause,
            command_name = command_name,
        )

        return Response (
            name = self.name,
            token = token,
            data = pack_to_any(fsm_result),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = [],
        )

    def advertise_control_address(self, address):
        self.uri = address

        if not self.connectivity_service:
            return

        self.logger.info(f'Registering {self.name} to the connectivity service at {address}')

        from threading import Thread
        self.running = True

        def update_connectivity_service(
            ctrler,
            connectivity_service,
            interval
        ):
            import time
            while ctrler.running:
                ctrler.connectivity_service.publish(
                    ctrler.name+"_control",
                    ctrler.uri,
                    'RunControlMessage',
                )
                time.sleep(interval)

        self.connectivity_service_thread = Thread(
            target = update_connectivity_service,
            args = (self, self.connectivity_service, 2),
            name = 'connectivity_service_updating_thread'
        )

        # lets roll
        self.connectivity_service_thread.start()


    def terminate(self):
        self.running = False

        if self.connectivity_service:
            if self.connectivity_service_thread:
                self.connectivity_service_thread.join()
            self.logger.info('Unregistering from the connectivity service')
            self.connectivity_service.retract(self.name+"_control")

        if self.can_broadcast():
            self.broadcast(
                btype = BroadcastType.SERVER_SHUTDOWN,
                message = 'over_and_out',
            )

        self.logger.info('Stopping children')
        for child in self.children_nodes:
            self.logger.debug(f'Stopping {child.name}')
            child.terminate()
        self.children_nodes = []

        from drunc.controller.children_interface.rest_api_child import ResponseListener

        if ResponseListener.exists():
            ResponseListener.get().terminate()

        import threading
        self.logger.debug("Threading threads")
        for t in threading.enumerate():
            self.logger.debug(f'{t.getName()} TID: {t.native_id} is_alive: {t.is_alive}')

        from multiprocessing import Manager
        with Manager() as manager:
            self.logger.debug("Multiprocess threads")
            self.logger.debug(manager.list())


    def __del__(self):
        self.terminate()

    def propagate_to_list(self, command:str, command_data, token, node_to_execute):

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_START,
            message = f'Propagating {command} to children',
        )

        response_children = []
        from threading import Lock, Thread
        response_lock = Lock()

        def propagate_to_child(child, command, command_data, token, response_lock, response_children):

            self.broadcast(
                btype = BroadcastType.CHILD_COMMAND_EXECUTION_START,
                message = f'Propagating {command} to children ({child.name})',
            )

            try:
                response = child.propagate_command(command, command_data, token)
                with response_lock:
                    response_children.append(response)

                if response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
                    self.broadcast(
                        btype = BroadcastType.CHILD_COMMAND_EXECUTION_SUCCESS,
                        message = f'Propagated {command} to children ({child.name}) successfully',
                    )
                else:
                    level = BroadcastType.DEBUG if response.flag == ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED else BroadcastType.CHILD_COMMAND_EXECUTION_FAILED
                    self.broadcast(
                        btype = level,
                        message = f'Propagating {command} to children ({child.name}) failed: {ResponseFlag.Name(response.flag)}. See its logs for more information and stacktrace.',
                    )

            except Exception as e: # Catch all, we are in a thread and want to do something sensible when an exception is thrown
                self.logger.error(f"Something wrong happened while sending the command to {child.name}: Error raised: {str(e)}")
                from drunc.utils.utils import print_traceback
                print_traceback()
                from drunc.exceptions import DruncException
                flag = ResponseFlag.DRUNC_EXCEPTION_THROWN if isinstance(e, DruncException) else ResponseFlag.UNHANDLED_EXCEPTION_THROWN

                with response_lock:
                    from druncschema.request_response_pb2 import Response
                    from druncschema.generic_pb2 import PlainText, Stacktrace
                    import traceback
                    stack = traceback.format_exc().split("\n")
                    response_children.append(
                        Response(
                            name = child.name,
                            token = token,
                            data = pack_to_any(
                                Stacktrace(
                                    text=stack
                                )
                            ),
                            flag = flag,
                            children = [],
                        )
                    )

                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_FAILED,
                    message = f'Failed to propagate {command} to {child.name} ({child.name}) EXCEPTION THROWN: {str(e)}',
                )

        threads = []
        for child in node_to_execute:
            self.logger.debug(f'Propagating to {child.name}')
            t = Thread(
                target = propagate_to_child,
                kwargs = {
                    "child": child,
                    "command": command,
                    "command_data": command_data,
                    "token": token,
                    "response_lock": response_lock,
                    "response_children": response_children,
                }
            )
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()
        return response_children


    ########################################################
    ############# Status, description commands #############
    ########################################################

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(pass_token=True) # 3rd step
    def get_children_status(self, token:Token) -> Response:
        #from drunc.controller.utils import get_status_message
        cs = []
        for n in self.children_nodes:
            try:
                cs += [n.get_status(token)]
            except Exception as e: # TEMPORARY hack
                from druncschema.controller_pb2 import Status
                cs += [
                    Status(
                        name = n.name,
                        state = 'unknown',
                        sub_state = 'unknown',
                        in_error = True,
                    )
                ]

        response =  ChildrenStatus(
            children_status = cs
        )
        return Response(
            name = self.name,
            token = None,
            data = pack_to_any(response),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = [],
        )

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    def get_status(self) -> Response:
        from drunc.controller.utils import get_status_message
        status = get_status_message(self.stateful_node)
        status.name = self.name

        return Response (
            name = self.name,
            token = None,
            data = pack_to_any(status),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = [],
        )


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    def ls(self) -> PlainTextVector:
        nodes = [node.name for node in self.children_nodes]
        response = PlainTextVector(
            text = nodes
        )

        return Response (
            name = self.name,
            token = None,
            data = pack_to_any(response),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = [],
        )


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None, pass_token=True) # 3rd step
    def describe(self, token:Token) -> Response:
        from druncschema.request_response_pb2 import Description
        from drunc.utils.grpc_utils import pack_to_any
        bd = self.describe_broadcast()
        d = Description(
            # endpoint = self.uri,
            type = 'controller',
            name = self.name,
            session = self.session,
            commands = self.commands,
            # children_endpoints = [child.get_endpoint() for child in self.children_nodes],
        )

        if bd:
            d.broadcast.CopyFrom(pack_to_any(bd))


        response_children = self.propagate_to_list(
            'describe',
            command_data = None,
            token = token,
            node_to_execute = self.children_nodes
        )

        return Response (
            name = self.name,
            token = None,
            data = pack_to_any(d),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = response_children,
        )

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(PlainText) # 4th step
    def describe_fsm(self, input:PlainText) -> Response:
        from drunc.fsm.utils import convert_fsm_transition

        if input.text == 'all-transitions':
            desc = convert_fsm_transition(self.stateful_node.get_all_fsm_transitions())
        elif input.text == '':
            desc = convert_fsm_transition(self.stateful_node.get_fsm_transitions())
        else:
            all_transitions = self.stateful_node.get_all_fsm_transitions()
            interesting_transitions = []
            for transition in all_transitions:
                if input.text == transition.source:
                    interesting_transitions += [transition]
                if input.text == transition.name:
                    interesting_transitions += [transition]
            desc = convert_fsm_transition(interesting_transitions)
        desc.type = 'controller'
        desc.name = self.name
        desc.session = self.session
        return Response (
            name = self.name,
            token = None,
            data = pack_to_any(desc),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = [],
        )


    ########################################
    ############# FSM commands #############
    ########################################
    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control # 3rd step
    @unpack_request_data_to(FSMCommand, pass_token=True) # 4th step
    def execute_fsm_command(self, fsm_command:FSMCommand, token:Token) -> Response:
        """
        A generic way to execute the controller commands from a user.
        1. Check if the command can be executed (correct FSM transition)
        2. Execute the command on children controller, app, and self
        3. Return the result
        """
        from druncschema.request_response_pb2 import ResponseFlag

        if self.stateful_node.node_is_in_error():
            return self.construct_error_node_response(
                fsm_command.command_name,
                token,
                cause = FSMResponseFlag.FSM_NOT_EXECUTED_IN_ERROR
            )

        if not self.stateful_node.node_is_included():
            self.logger.error(f"Node is not included, not executing command {fsm_command.command_name}.")
            fsm_result = FSMCommandResponse(
                flag = FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED,
                command_name = fsm_command.command_name,
            )

            return Response (
                name = self.name,
                token = token,
                data = pack_to_any(fsm_result),
                flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
                children = [],
            )


        transition = self.stateful_node.get_fsm_transition(fsm_command.command_name)

        self.logger.debug(f'The transition requested is "{str(transition)}"')

        if not self.stateful_node.can_transition(transition):
            self.logger.error(f'Cannot \"{transition.name}\" as this is an invalid command in state \"{self.stateful_node.node_operational_state()}\"')

            fsm_result = FSMCommandResponse(
                flag = FSMResponseFlag.FSM_INVALID_TRANSITION,
                command_name = fsm_command.command_name,
            )

            return Response (
                name = self.name,
                token = token,
                data = pack_to_any(fsm_result),
                flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
                children = [],
            )

        self.logger.debug(f'FSM command data: {fsm_command}')

        fsm_args = self.stateful_node.decode_fsm_arguments(fsm_command)

        fsm_data = self.stateful_node.prepare_transition(
            transition = transition,
            transition_args = fsm_args,
            transition_data = fsm_command.data,
            ctx = self,
        )

        self.stateful_node.propagate_transition_mark(transition)

        children_fsm_command = FSMCommand()
        children_fsm_command.CopyFrom(fsm_command)
        children_fsm_command.data = fsm_data
        children_fsm_command.ClearField("children_nodes") # we strip the children node, since when we feed them to the children they are meaningless
        execute_on = fsm_command.children_nodes

        response_children = self.propagate_to_list(
            'execute_fsm_command',
            command_data = children_fsm_command,
            token = token,
            node_to_execute = self.children_nodes,
        )

        child_worst_response_flag = ResponseFlag.EXECUTED_SUCCESSFULLY
        child_worst_fsm_flag = FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY

        for response_child in response_children:

            if response_child.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
                child_worst_response_flag = response_child.flag
                continue

            from drunc.utils.grpc_utils import unpack_any
            fsm_response = unpack_any(response_child.data, FSMCommandResponse)

            if fsm_response.flag != FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY:
                child_worst_fsm_flag = fsm_response.flag


        self.stateful_node.finish_propagating_transition_mark(transition)

        self.stateful_node.start_transition_mark(transition)

        self.stateful_node.terminate_transition_mark(transition)

        fsm_data = self.stateful_node.finalise_transition(
            transition = transition,
            transition_args = fsm_args,
            transition_data = fsm_data,
            ctx = self,
        )

        if (child_worst_response_flag != ResponseFlag.EXECUTED_SUCCESSFULLY or
            child_worst_fsm_flag != FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY):

            self.stateful_node.to_error()

        #     return self.construct_error_node_response(
        #         fsm_command.command_name,
        #         token,
        #         cause = FSMResponseFlag.FSM_FAILED,
        #     )

        self_response_fsm_flag = FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY # self has executed successfully, even if children have not
        fsm_result = FSMCommandResponse(
            flag = self_response_fsm_flag,
            command_name = fsm_command.command_name,
        )

        return Response (
            name = self.name,
            token = token,
            data = pack_to_any(fsm_result),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = response_children,
        )


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control # 3rd step
    @unpack_request_data_to(pass_token=True) # 4th step
    def include(self, token:Token) -> PlainText:
        response_children = self.propagate_to_list('include', command_data=None, token=token, node_to_execute=self.children_nodes)
        self.stateful_node.include_node()
        resp = PlainText(text = f'{self.name} and children included')

        return Response (
            name = self.name,
            token = token,
            data = pack_to_any(resp),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = response_children,
        )


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control
    @unpack_request_data_to(pass_token=True) # 3rd step
    def exclude(self, token:Token) -> Response:
        response_children = self.propagate_to_list('exclude', command_data=None, token=token, node_to_execute=self.children_nodes)
        self.stateful_node.exclude_node()
        resp =  PlainText(text = f'{self.name} and children excluded')
        return Response (
            name = self.name,
            token = token,
            data = pack_to_any(resp),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = response_children,
        )



    ##########################################
    ############# Actor commands #############
    ##########################################

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(pass_token=True) # 3rd step
    def take_control(self, token:Token) -> PlainText:
        if self.actor.take_control(token) != 0:
            return Response(
                name = self.name,
                token = token,
                data = pack_to_any(
                    PlainText(
                        text='Could not take control'
                    )
                ),
                flag = ResponseFlag.FAILED,
                children = [],
            )

        response_children = self.propagate_to_list('take_control', command_data=None, token=token, node_to_execute=self.children_nodes)
        if any(cr.flag not in [ResponseFlag.EXECUTED_SUCCESSFULLY, ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED] for cr in response_children):
            return Response(
                name = self.name,
                token = token,
                data = pack_to_any(
                    PlainText(
                        text='Could not take control on all children'
                    )
                ),
                flag = ResponseFlag.FAILED,
                children = response_children,
            )

        resp = PlainText(text = f'{token.user_name} took control')
        return Response(
            name = self.name,
            token = token,
            data = pack_to_any(resp),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = response_children,
        )

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control # 3rd step
    @unpack_request_data_to(pass_token=True) # 4th step
    def surrender_control(self, token:Token) -> PlainText:
        user = self.actor.get_user_name()
        if self.actor.surrender_control(token) != 0:
            return Response(
                name = self.name,
                token = token,
                data = pack_to_any(
                    PlainText(
                        text='Could not surrender control'
                    )
                ),
                flag = ResponseFlag.FAILED,
                children = [],
            )

        response_children = self.propagate_to_list('surrender_control', command_data=None, token=token, node_to_execute=self.children_nodes)
        if any(cr.flag not in [ResponseFlag.EXECUTED_SUCCESSFULLY, ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED] for cr in response_children):
            return Response(
                name = self.name,
                token = token,
                data = pack_to_any(
                    PlainText(
                        text='Could not surrender control on all children'
                    )
                ),
                flag = ResponseFlag.FAILED,
                children = response_children,
            )

        resp = PlainText(text = f'{user} surrendered control')
        return Response(
            name = self.name,
            token = token,
            data = pack_to_any(resp),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = response_children,
        )

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    def who_is_in_charge(self) -> PlainText:
        user = self.actor.get_user_name()
        return Response (
            name = self.name,
            token = None,
            data = pack_to_any(PlainText(text=user)),
            flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
            children = [],
        )
