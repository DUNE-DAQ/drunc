from threading import Lock


class ClientSideState:
    def __init__(self, initial_state: str = "initial") -> None:
        # We'll wrap all these in a mutex for good measure
        self._state_lock = Lock()
        self._executing_command = False
        self._assumed_operational_state = initial_state
        self._included = True
        self._errored = False

    def executing_command_mark(self) -> None:
        with self._state_lock:
            self._executing_command = True

    def end_command_execution_mark(self) -> None:
        with self._state_lock:
            self._executing_command = False

    def new_operational_state(self, new_state: str) -> None:
        with self._state_lock:
            self._assumed_operational_state = new_state

    def get_operational_state(self) -> str:
        with self._state_lock:
            return self._assumed_operational_state

    def get_executing_command(self) -> bool:
        with self._state_lock:
            return self._executing_command

    def include(self) -> None:
        with self._state_lock:
            self._included = True

    def exclude(self) -> None:
        with self._state_lock:
            self._included = False

    def included(self) -> bool:
        with self._state_lock:
            return self._included

    def excluded(self) -> bool:
        with self._state_lock:
            return not self._included

    def to_error(self) -> None:
        with self._state_lock:
            self._errored = True

    def fix_error(self) -> None:
        with self._state_lock:
            self._errored = False

    def in_error(self) -> bool:
        with self._state_lock:
            return self._errored
