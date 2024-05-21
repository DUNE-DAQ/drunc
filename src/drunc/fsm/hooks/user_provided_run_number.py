from drunc.fsm.core import FSMHook

class UserProvidedRunNumber(FSMHook):
    def __init__(self, configuration):
        super().__init__(
            name = "run-number"
        )

    def pre_start(self, _input_data:dict, run_number:int=1, disable_data_storage:bool=False, trigger_rate:float=1.0, **kwargs):
        _input_data["run"] = run_number
        _input_data['disable_data_storage'] = disable_data_storage
        _input_data['trigger_rate'] = trigger_rate
        return _input_data
