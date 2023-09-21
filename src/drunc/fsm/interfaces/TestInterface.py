from drunc.fsm.fsm_core import FSMInterface
from enum import Enum

class an_enum(Enum):
    ONE=1
    TWO=2

class TestInterface(FSMInterface):
    def __init__(self, configuration):
        super(TestInterface, self).__init__(
            name = "test-interface"
        )


    def pre_conf(self, _input_data:dict, some_int:int, some_str:str, some_float:float=0.2, **kwargs) -> dict:
        print(f"Running pre_conf of {self.name}")
        _input_data['some_int'] = some_int
        _input_data['some_str'] = some_str
        _input_data['some_float'] = some_float
        return _input_data
