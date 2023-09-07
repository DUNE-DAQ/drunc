from google.rpc import code_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

import traceback

from druncschema.request_response_pb2 import Request, Response
from druncschema.token_pb2 import Token
from druncschema.generic_pb2 import PlainText, PlainTextVector, Stacktrace
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



class Controller(StatefulNode, ControllerServicer, BroadcastSender):
    def __init__(self, configuration:str, **kwargs):
        from drunc.controller.configuration import ControllerConfiguration
        self.configuration = ControllerConfiguration(configuration)

        super(Controller, self).__init__(
            broadcast_configuration = self.configuration.get('broadcaster'),
            statefulnode_configuration = self.configuration.get('statefulnode'),
            **kwargs
        )

        self.children_nodes = [] # type: List[ChildNode]

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import SystemType
        self.authoriser = DummyAuthoriser({}, SystemType.CONTROLLER)

        self.actor = ControllerActor(None)

        self.controller_token = Token(
            user_name = f'{self.name}_controller',
            token = 'broadcast_token' # massive hack here, controller should use user token to execute command, and have a "broadcasted to" token
        )

        for child_controller_cfg in self.configuration.get('children_controllers', []):
            self.children_nodes.append(
                ChildNode.get_from_file(
                        child_controller_cfg['name'],
                        child_controller_cfg
                    )
                )

        for app_cfg in self.configuration.get('applications', []):
            self.children_nodes.append(ChildNode.get_from_file(app_cfg))

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
                data_type = ['controller_pb2.FSMCommand'],
                help = 'Include self in the current session, if a children is provided, include it and its eventual children',
                return_type = 'controller_pb2.FSMCommandResponse'
            ),

            CommandDescription(
                name = 'exclude',
                data_type = ['controller_pb2.FSMCommand'],
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
        self.broadcast(
            btype = BroadcastType.SERVER_SHUTDOWN,
            message = 'over_and_out',
        )

        self.logger.info('Stopping children')
        for child in self.children_nodes:
            self.logger.debug(f'Stopping {child.name}')
            child.terminate()


    def propagate_to_list(self, command:str, data, token, node_to_execute):

        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_START,
            message = f'Propagating {command} to children',
        )

        def propagate_to_child(child, command, data, token):

            self.broadcast(
                btype = BroadcastType.CHILD_COMMAND_EXECUTION_START,
                message = f'Propagating {command} to children ({child.name})',
            )

            try:
                child.propagate_command(command, data, token)
                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_SUCCESS,
                    message = f'Propagating {command} to children ({child.name})',
                )
            except:
                self.broadcast(
                    btype = BroadcastType.CHILD_COMMAND_EXECUTION_FAILED,
                    message = f'Failed to propagate {command} to {child.name} ({child.name})',
                )

        threads = []
        for child in node_to_execute:
            self.logger.debug(f'Propagating to {child.name}')
            t = Thread(target=propagate_to_child, args=(child, command, data, token))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()


    def _should_execute_on_self(self, node_path) -> bool:
        if node_path == []:
            return True

        for node in node_path:
            if node == [self.name]:
                return True
        return False


    def _interrupt_with_message(self, message, context):
        detail = pack_to_any(PlainText(text = message))

        context.abort_with_status(
            rpc_status.to_status(
                status_pb2.Status(
                    code=code_pb2.INTERNAL,
                    message=message,
                    details=[detail],
                )
            )
        )


    def _interrupt_with_exception(self, ex_stack, ex_text, context):
        self.logger.error(
            ex_stack+"\n"+ex_text
        )

        detail = pack_to_any(
            Stacktrace(
                text = ex_stack.split('\n')
            )
        )
        context.abort_with_status(
            rpc_status.to_status(
                status_pb2.Status(
                    code=code_pb2.INTERNAL,
                    message=f'Exception thrown: {str(ex_text)}',
                    details=[detail],
                )
            )
        )


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
            self.broadcast(
                btype = BroadcastType.TEXT_MESSAGE, # make this an real type rather than just text
                message = message
            )
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
            self.broadcast(
                btype = BroadcastType.EXCEPTION_RAISED,
                message = f'ControllerException when executing {command}: {e}'
            )

            self._interrupt_with_exception(
                ex_stack = traceback.format_exc(),
                ex_text = str(e),
                context = context
            )

        except Exception as e:
            self.broadcast(
                btype = BroadcastType.UNHANDLED_EXCEPTION_RAISED,
                message = f'Unhandled exception when executing {command}: {e}'
            )

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
            self.broadcast(
                btype = BroadcastType.TEXT_MESSAGE, # make this an real type rather than just text
                message= message
            )
            self._interrupt_with_message(message, context)


        self.logger.debug(f'{command} data: {fsm_command}')
        #def propagate_to_list(self, command:str, data, token, node_to_execute):
        if False: # Keep this out of the way for now
            children_fsm_command = FSMCommand()
            children_fsm_command.CopyFrom(fsm_command)
            children_fsm_command.ClearField("children_nodes") # we strip the children node, since when we feed them to the children they are meaningless
            if fsm_command.HasField('children_nodes'):
                self.propagate_to_list(command, children_fsm_command, request.token, fsm_command.children_nodes)
            else:
                self.propagate_to_list(command, children_fsm_command, request.token, self.children_nodes)

        import drunc.fsm.fsm_errors as fsm_errors
        self.broadcast(
            btype = BroadcastType.COMMAND_EXECUTION_START,
            message = f'Executing {fsm_command.command_name} (upon request from {request.token.user_name})',
        )

        try:
            result = self.fsm.execute_transition(fsm_command.command_name, fsm_command.arguments)

        except fsm_errors.UnregisteredTransition as e:
            self.broadcast(
                btype = BroadcastType.EXCEPTION_RAISED,
                message = f'Transition {fsm_command.command_name} not a valid transition. Available transitions are {[self.fsm.get_all_transitions()]}.'
            )
            self._interrupt_with_exception(
                ex_stack = traceback.format_exc(),
                ex_text = str(e),
                context = context
            )
        except fsm_errors.InvalidTransition as e:
            self.broadcast(
                btype = BroadcastType.EXCEPTION_RAISED,
                message = f'Transition {fsm_command.command_name} is invalid from state {self.fsm.get_current_state()}. Available transitions are {[self.fsm.get_executable_transitions()]}.'
            )
            self._interrupt_with_exception(
                ex_stack = traceback.format_exc(),
                ex_text = str(e),
                context = context
            )
        except ctler_excpt.ControllerException as e:
            self.broadcast(
                btype = BroadcastType.EXCEPTION_RAISED,
                message = f'ControllerException when executing {fsm_command.command_name}: {e}'
            )
            self._interrupt_with_exception(
                ex_stack = traceback.format_exc(),
                ex_text = str(e),
                context = context
            )
        except Exception as e:
            self.broadcast(
                btype = BroadcastType.UNHANDLED_EXCEPTION_RAISED,
                message = f'Unhandled exception when executing {command}: {e}'
            )
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



    ########################################################
    ############# Status, description commands #############
    ########################################################

    def get_children_status(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'get_children_status', context, propagate=False)

    def _get_children_status_impl(self, _, dummy) -> ChildrenStatus:
        from drunc.controller.utils import get_status_message
        return ChildrenStatus(
            children_status = [get_status_message(n) for n in self.children_nodes]
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
        return Description(
            type = 'controller',
            name = self.name,
            session = self.session,
            commands = self.commands
        )


    def describe_fsm(self, request:Request, context) -> Response:
        return self._generic_user_command(request, 'describe_fsm', context, propagate=False)

    def _describe_fsm_impl(self, _, dummy):
        from drunc.fsm.utils import convert_fsm_transition
        desc = convert_fsm_transition(self.fsm)
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
        return self._fsm_command(request, 'include', context)


    def exclude(self, request:Request, context) -> Response:
        return self._fsm_command(request, 'exclude', context)



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
