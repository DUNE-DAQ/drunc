import traceback

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
        if not self.compare_token(self._token, Token()):
            raise ctler_excpt.OtherUserAlreadyInControl(f'Actor {self._token.user_name} is already in control')

        self._update_actor(token)



class Controller(StatefulNode, ControllerServicer, BroadcastSender):
    def __init__(self, configuration:str, **kwargs):
        from drunc.controller.configuration import ControllerConfiguration
        self.configuration = ControllerConfiguration(configuration)
        self.children_nodes = [] # type: List[ChildNode]

        super(Controller, self).__init__(
            broadcast_configuration = self.configuration.get('broadcaster'),
            statefulnode_configuration = self.configuration.get('statefulnode'),
            **kwargs
        )


        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import SystemType
        self.authoriser = DummyAuthoriser({}, SystemType.CONTROLLER)

        self.actor = ControllerActor(None)

        self.controller_token = Token(
            user_name = f'{self.name}_controller',
            token = 'broadcast_token' # massive hack here, controller should use user token to execute command, and have a "broadcasted to" token
        )


        from copy import deepcopy as dc
        fsm_conf = dc(self.configuration.data['statefulnode']['fsm'])
        fsm_conf.update({
            "interfaces": {},
            "pre_transitions": {},
            "post_transitions": {},
        })

        for child in self.configuration.get('children', []):
            if child['type'] == 'rest-api': # already some hacking
                self.children_nodes.append(
                    ChildNode.get_from_file(
                            name = child['name'],
                            conf = child,
                            fsm_conf = fsm_conf
                        )
                    )
            else:
                self.children_nodes.append(
                    ChildNode.get_from_file(
                            name = child['name'],
                            conf = child,
                        )
                    )

        # for app_cfg in self.configuration.get('applications', []):
        #     self.children_nodes.append(ChildNode.get_from_file(app_cfg))

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
        self.logger.info("Threading threads")
        for t in threading.enumerate():
            print(f'{t.getName()} TID: {t.native_id} is_alive: {t.is_alive}')

        from multiprocessing import Manager
        with Manager() as manager:
            self.logger.info("Multiprocess threads")
            self.logger.info(manager.list())


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
            message = f'{request.token.user_name} is not authorised to execute {command}'
            self._interrupt_with_message(message, context)


        data = request.data if request.data else None
        self.logger.debug(f'{command} data: {request.data}')


        if propagate:
            self.propagate_to_list(command, request.data, request.token, self.children_nodes)

        try:
            token = Token()
            token.CopyFrom(request.token)
            self.broadcast(
                btype = BroadcastType.COMMAND_EXECUTION_START,
                message = f'Executing {command} (upon request from {request.token.user_name})',
            )
            result = getattr(self, "_"+command+"_impl")(data, token)

        except ctler_excpt.ControllerException as e:
            self._interrupt_with_message(
                str(e),
                context = context
            )

        except Exception as e:
            self._interrupt_with_exception(
                ex_stack = traceback.format_exc(),
                ex_text = str(e),
                context = context
            )

        result_any = pack_to_any(result)
        response = Response(data = result_any)

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS,
            message = f'Succesfully executed {command}'
        )

        return response



    def _fsm_command(self, request:Request, command:str, context):
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
            message = f'{request.token.user_name} attempting to execute and FSM command: {command}'
        )
        from drunc.utils.grpc_utils import unpack_any
        from druncschema.controller_pb2 import FSMCommand
        fsm_command = unpack_any(request.data, FSMCommand)

        if not self.authoriser.is_authorised(request.token, 'fsm'):
            message = f'{request.token.user_name} is not authorised to execute {fsm_command.command_name}'
            self._interrupt_with_message(message, context)

        if not self.actor.token_is_current_actor(request.token):
            message = f'{request.token.user_name} is not in control (ask "{self.actor.get_user_name()}" to surrender control to execute the command)'
            self._interrupt_with_message(message, context)

        if not self.node_is_included():
            message = f'{self.name} is not included, not executing the FSM command {fsm_command.command_name}'
            self._interrupt_with_message(message, context)

        transition = self.get_fsm_transition(fsm_command.command_name)

        self.logger.debug(f'The transition requested is "{str(transition)}"')

        if not self.can_transition(transition):
            message = f'Cannot \"{transition.name}\" as this is an invalid command in state \"{self.node_operational_state()}\"'
            self._interrupt_with_message(message, context)


        self.logger.debug(f'{command} data: {fsm_command}')
        child_statuses = {}
        try:

            fsm_args = self.decode_fsm_arguments(fsm_command)

            fsm_data = self.prepare_transition(
                transition = transition,
                transition_args = fsm_args,
                transition_data = fsm_command.data,
            )

            self.propagate_transition_mark(transition)

            children_fsm_command = FSMCommand()
            children_fsm_command.CopyFrom(fsm_command)
            children_fsm_command.data = fsm_data
            children_fsm_command.ClearField("children_nodes") # we strip the children node, since when we feed them to the children they are meaningless
            execute_on = fsm_command.children_nodes

            if execute_on:
                child_statuses = self.propagate_to_list(command, children_fsm_command, request.token, execute_on)
            else:
                child_statuses = self.propagate_to_list(command, children_fsm_command, request.token, self.children_nodes)

            self.finish_propagating_transition_mark(transition)

            self.start_transition_mark(transition)

            self.broadcast(
                btype = BroadcastType.COMMAND_EXECUTION_START,
                message = f'Executing {fsm_command.command_name} (upon request from {request.token.user_name})',
            )

            self.terminate_transition_mark(transition)

            fsm_data = self.finalise_transition(
                transition = transition,
                transition_args = fsm_args,
                transition_data = fsm_data
            )

        except Exception as e:
            self._interrupt_with_exception(
                ex_stack = traceback.format_exc(),
                ex_text = str(e),
                context = context
            )

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS,
            message = f'Succesfully executed {command}'
        )
        from druncschema.controller_pb2 import FSMCommandResponse, FSMCommandResponseCode

        result = FSMCommandResponse(
            successful = FSMCommandResponseCode.SUCCESSFUL,
            command_name = fsm_command.command_name,
            children_successful = child_statuses,
        )

        result_any = pack_to_any(result)

        response = Response(
            data = result_any
        )

        response.token.CopyFrom(request.token)

        return response



    ########################################################
    ############# Status, description commands #############
    ########################################################

    def get_children_status(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'get_children_status', context, propagate=False)

    def _get_children_status_impl(self, _, token) -> ChildrenStatus:
        #from drunc.controller.utils import get_status_message
        return ChildrenStatus(
            children_status = [n.get_status(token) for n in self.children_nodes]
        )


    def get_status(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'get_status', context, propagate=False)

    def _get_status_impl(self, _, dummy) -> Status:
        from drunc.controller.utils import get_status_message
        return get_status_message(self)


    def ls(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'ls', context, propagate=False)

    def _ls_impl(self, _, dummy) -> PlainTextVector:
        nodes = [node.name for node in self.children_nodes]
        return PlainTextVector(
            text = nodes
        )


    def describe(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'describe', context, propagate=False)

    def _describe_impl(self, request:Request, dummy):
        from druncschema.request_response_pb2 import Description
        from drunc.utils.grpc_utils import pack_to_any
        return Description(
            type = 'controller',
            name = self.name,
            session = self.session,
            commands = self.commands,
            broadcast = pack_to_any(self.describe_broadcast()),
        )


    def describe_fsm(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'describe_fsm', context, propagate=False)

    def _describe_fsm_impl(self, _, dummy):
        from drunc.fsm.utils import convert_fsm_transition
        desc = convert_fsm_transition(self.get_fsm_transitions())
        desc.type = 'controller'
        desc.name = self.name
        desc.session = self.session
        return desc


    ########################################
    ############# FSM commands #############
    ########################################

    def execute_fsm_command(self, request:Request, context) -> Response:
        return self._fsm_command(request, 'execute_fsm_command', context)


    def include(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'include', context, propagate=True)

    def _include_impl(self, _, token) -> PlainText:
        self.include_node()
        return PlainText(text = f'{self.name} included')


    def exclude(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'exclude', context, propagate=True)

    def _exclude_impl(self, _, token) -> PlainText:
        self.exclude_node()
        return PlainText(text = f'{self.name} excluded')


    ##########################################
    ############# Actor commands #############
    ##########################################

    def take_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'take_control', context, propagate=True)

    def _take_control_impl(self, _, token) -> PlainText:
        self.actor.take_control(token)
        return PlainText(text = f'{token.user_name} took control')


    def surrender_control(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'surrender_control', context, propagate=True)

    def _surrender_control_impl(self, _, token) -> PlainText:
        user = self.actor.get_user_name()
        self.actor.surrender_control(token)
        return PlainText(text = f'{user} surrendered control')


    def who_is_in_charge(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'who_is_in_charge', context)

    def _who_is_in_charge_impl(self, *args) -> PlainText:
        user = self.actor.get_user_name()
        return PlainText(text = user)
