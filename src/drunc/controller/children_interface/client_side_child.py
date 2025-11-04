from threading import Lock

from druncschema.controller_pb2 import AddressedCommand
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token

from drunc.controller.children_interface.child_node import ChildNode
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.core import FSM
from drunc.utils.grpc_utils import pack_to_any, unpack_any
from drunc.utils.utils import ControlType, get_logger


class ClientSideState:
    def __init__(self, initial_state="initial"):
        # We'll wrap all these in a mutex for good measure
        self._state_lock = Lock()
        self._executing_command = False
        self._assumed_operational_state = initial_state
        self._included = True
        self._errored = False

    def executing_command_mark(self):
        with self._state_lock:
            self._executing_command = True

    def end_command_execution_mark(self):
        with self._state_lock:
            self._executing_command = False

    def new_operational_state(self, new_state):
        with self._state_lock:
            self._assumed_operational_state = new_state

    def get_operational_state(self):
        with self._state_lock:
            return self._assumed_operational_state

    def get_executing_command(self):
        with self._state_lock:
            return self._executing_command

    def include(self):
        with self._state_lock:
            self._included = True

    def exclude(self):
        with self._state_lock:
            self._included = False

    def included(self):
        with self._state_lock:
            return self._included

    def excluded(self):
        with self._state_lock:
            return not self._included

    def to_error(self):
        with self._state_lock:
            self._errored = True

    def fix_error(self):
        with self._state_lock:
            self._errored = False

    def in_error(self):
        with self._state_lock:
            return self._errored


class ClientSideChild(ChildNode):
    def __init__(
        self,
        name,
        node_type: ControlType = ControlType.Direct,
        fsm_configuration: FSMConfHandler = None,
        configuration=None,
    ):  #
        super().__init__(
            name=name,
            node_type=node_type,
            configuration=configuration,
        )
        self.log = get_logger(f"controller.{name}-client-side")
        self.state = ClientSideState()
        self.fsm_configuration = fsm_configuration
        if fsm_configuration:
            fsmch = FSMConfHandler(fsm_configuration)
            self.fsm = FSM(conf=fsmch)

    def propagate_command(
        self,
        command: str,
        request: AddressedCommand,
        token: Token | None,
    ) -> Response:
        if command == "exclude":
            self.state.exclude()
            return Response(
                name=self.name,
                data=pack_to_any(PlainText(text=f"'{self.name}' excluded")),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            )

        if command == "include":
            self.state.include()
            return Response(
                name=self.name,
                data=pack_to_any(PlainText(text=f"'{self.name}' included")),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            )

        if command == "execute_expert_command":
            return self.propagate_expert_command(
                unpack_any(request.command_data, PlainText), None
            )

        # If we get here, we don't run the command.
        self.log.info(f"Ignoring command '{command}' sent to '{self.name}'")
        return Response(
            name=self.name,
            flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
        )

    def propagate_expert_command(self, data: PlainText, token: Token) -> Response:
        return Response(
            name=self.name,
            flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
        )
