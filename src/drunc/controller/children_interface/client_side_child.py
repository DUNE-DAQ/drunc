from threading import Lock

from drunc.controller.children_interface.child_node import ChildNode
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.core import FSM
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
    ):
        super().__init__(name=name, node_type=node_type, configuration=configuration)
        self.log = get_logger(f"controller.{name}-client-side")
        self.state = ClientSideState()
        self.fsm_configuration = fsm_configuration
        if fsm_configuration:
            fsmch = FSMConfHandler(fsm_configuration)
            self.fsm = FSM(conf=fsmch)
