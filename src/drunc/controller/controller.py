from druncschema.request_response_pb2 import Request, Response
from druncschema.token_pb2 import Token
from druncschema.generic_pb2 import PlainText, PlainTextVector
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.controller_pb2_grpc import ControllerServicer
from druncschema.controller_pb2 import Status, ChildrenStatus

from drunc.broadcast.server.broadcast_sender import BroadcastSender

from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.stateful_node import StatefulNode
from drunc.broadcast.server.broadcast_sender import BroadcastSender
import drunc.controller.exceptions as ctler_excpt
from drunc.utils.grpc_utils import pack_to_any
from threading import Lock, Thread
from typing import Optional, List
from drunc.broadcast.server.decorators import broadcasted
from drunc.utils.grpc_utils import unpack_request_data_to, pack_response
from drunc.authoriser.decorators import authentified_and_authorised
from druncschema.authoriser_pb2 import ActionType, SystemType
from drunc.controller.decorators import in_control

from druncschema.controller_pb2 import FSMCommand

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
        return token1.user_name == token2.user_name and token1.token == token2.token #!! come on protobuf, you can compare messages

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

        self.children_nodes = self.configuration.get_children()
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
                data_type = ['None'],
                help = 'List available FSM commands for the current state.',
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

    def terminate(self):

        if self.can_broadcast():
            self.broadcast(
                btype = BroadcastType.SERVER_SHUTDOWN,
                message = 'over_and_out',
            )

        self.logger.info('Stopping children')
        for child in self.children_nodes:
            self.logger.debug(f'Stopping {child.name}')
            child.terminate()

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

    def propagate_to_list(self, command:str, data, token, node_to_execute):

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_START,
            message = f'Propagating {command} to children',
        )
        return_statuses = {}
        def propagate_to_child(child, command, data, token):

            self.broadcast(
                btype = BroadcastType.CHILD_COMMAND_EXECUTION_START,
                message = f'Propagating {command} to children ({child.name})',
            )

            try:
                return_statuses[child.name] = child.propagate_command(command, data, token)
                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_SUCCESS,
                    message = f'Propagating {command} to children ({child.name})',
                )
            except Exception as e:
                from druncschema.controller_pb2 import FSMCommandResponseCode
                return_statuses[child.name] = FSMCommandResponseCode.UNSUCCESSFUL
                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_FAILED,
                    message = f'Failed to propagate {command} to {child.name} ({child.name}): {str(e)}',
                )

        threads = []
        for child in node_to_execute:
            self.logger.debug(f'Propagating to {child.name}')
            t = Thread(target=propagate_to_child, args=(child, command, data, token))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()
        return return_statuses


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
    @pack_response # 4th step
    def get_children_status(self, token:Token) -> ChildrenStatus:
        #from drunc.controller.utils import get_status_message
        return ChildrenStatus(
            children_status = [n.get_status(token) for n in self.children_nodes]
        )

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    @pack_response # 4th step
    def get_status(self) -> Status:
        from drunc.controller.utils import get_status_message
        status = get_status_message(self.stateful_node)
        status.name = self.name
        return status


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    @pack_response # 4th step
    def ls(self) -> PlainTextVector:
        nodes = [node.name for node in self.children_nodes]
        return PlainTextVector(
            text = nodes
        )


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    @pack_response # 4th step
    def describe(self) -> Response:
        from druncschema.request_response_pb2 import Description
        from drunc.utils.grpc_utils import pack_to_any
        bd = self.describe_broadcast()
        d = Description(
            type = 'controller',
            name = self.name,
            session = self.session,
            commands = self.commands,
        )
        if bd:
            d.broadcast.CopyFrom(pack_to_any(bd))
        return d


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    @pack_response # 4th step
    def describe_fsm(self) -> Response:
        from drunc.fsm.utils import convert_fsm_transition
        desc = convert_fsm_transition(self.stateful_node.get_fsm_transitions())
        desc.type = 'controller'
        desc.name = self.name
        desc.session = self.session
        return desc


    ########################################
    ############# FSM commands #############
    ########################################
    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control
    @unpack_request_data_to(FSMCommand, pass_token=True) # 3rd step
    @pack_response # 4th step
    def execute_fsm_command(self, fsm_command:FSMCommand, token:Token) -> Response:
        """
        A generic way to execute the controller commands from a user.
        1. Check if the command can be executed (correct FSM transition)
        2. Execute the command on children controller, app, and self
        3. Return the result
        """

        transition = self.stateful_node.get_fsm_transition(fsm_command.command_name)

        self.logger.debug(f'The transition requested is "{str(transition)}"')

        if not self.stateful_node.can_transition(transition):
            message = f'Cannot \"{transition.name}\" as this is an invalid command in state \"{self.stateful_node.node_operational_state()}\"'

        self.logger.debug(f'FSM command data: {fsm_command}')
        child_statuses = {}

        fsm_args = self.stateful_node.decode_fsm_arguments(fsm_command)

        fsm_data = self.stateful_node.prepare_transition(
            transition = transition,
            transition_args = fsm_args,
            transition_data = fsm_command.data,
        )

        self.stateful_node.propagate_transition_mark(transition)

        children_fsm_command = FSMCommand()
        children_fsm_command.CopyFrom(fsm_command)
        children_fsm_command.data = fsm_data
        children_fsm_command.ClearField("children_nodes") # we strip the children node, since when we feed them to the children they are meaningless
        execute_on = fsm_command.children_nodes

        if execute_on:
            child_statuses = self.propagate_to_list('execute_fsm_command', children_fsm_command, token, execute_on)
        else:
            child_statuses = self.propagate_to_list('execute_fsm_command', children_fsm_command, token, self.children_nodes)

        self.stateful_node.finish_propagating_transition_mark(transition)

        self.stateful_node.start_transition_mark(transition)

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_START,
            message = f'Executing {fsm_command.command_name} (upon request from {token.user_name})',
        )

        self.stateful_node.terminate_transition_mark(transition)

        fsm_data = self.stateful_node.finalise_transition(
            transition = transition,
            transition_args = fsm_args,
            transition_data = fsm_data
        )

        from druncschema.controller_pb2 import FSMCommandResponse, FSMCommandResponseCode

        result = FSMCommandResponse(
            successful = FSMCommandResponseCode.SUCCESSFUL,
            command_name = fsm_command.command_name,
            children_successful = child_statuses,
        )

        return result


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control
    @unpack_request_data_to(pass_token=True) # 3rd step
    @pack_response # 4th step
    def include(self, token:Token) -> PlainText:
        self.propagate_to_list('include', data=None, token=token, node_to_execute=self.children_nodes)
        self.stateful_node.include_node()
        return PlainText(text = f'{self.name} and children included')


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @in_control
    @unpack_request_data_to(pass_token=True) # 3rd step
    @pack_response # 4th step
    def exclude(self, token:Token) -> Response:
        self.propagate_to_list('exclude', data=None, token=token, node_to_execute=self.children_nodes)
        self.stateful_node.exclude_node()
        return PlainText(text = f'{self.name} and children excluded')


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
    @pack_response # 4th step
    def take_control(self, token:Token) -> PlainText:
        self.actor.take_control(token)
        self.propagate_to_list('take_control', data=None, token=token, node_to_execute=self.children_nodes)
        return PlainText(text = f'{token.user_name} took control')

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(pass_token=True) # 3rd step
    @pack_response # 4th step
    def surrender_control(self, token:Token) -> PlainText:
        user = self.actor.get_user_name()
        self.actor.surrender_control(token)
        self.propagate_to_list('surrender_control', data=None, token=token, node_to_execute=self.children_nodes)
        return PlainText(text = f'{user} surrendered control')

    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.CONTROLLER
    ) # 2nd step
    @unpack_request_data_to(None) # 3rd step
    @pack_response # 4th step
    def who_is_in_charge(self) -> PlainText:
        user = self.actor.get_user_name()
        return PlainText(text = user)
