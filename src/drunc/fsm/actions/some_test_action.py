from enum import Enum

from drunc.fsm._protocols import ConfigurationProtocol, ContextProtocol
from drunc.fsm.core import FSMAction


class an_enum(Enum):
    ONE = 1
    TWO = 2


class SomeTestAction(FSMAction):
    def __init__(self, configuration: ConfigurationProtocol) -> None:
        super().__init__(name="test-action")

    def pre_conf(
        self,
        _input_data: dict[str, object],
        _context: ContextProtocol,
        some_int: int,
        some_str: str,
        some_float: float = 0.2,
        **kwargs: object,
    ) -> dict[str, object]:
        print(f"Running pre_conf of {self.name}")
        _input_data["some_int"] = some_int
        _input_data["some_str"] = some_str
        _input_data["some_float"] = some_float
        return _input_data
