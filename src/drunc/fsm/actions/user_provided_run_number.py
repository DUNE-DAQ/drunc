from drunc.fsm.core import FSMAction

class UserProvidedRunNumber(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "run-number"
        )

    def pre_start(self, _input_data:dict, _context, run_number:int, disable_data_storage:bool=False, trigger_rate:float=0., run_type:str='TEST', **kwargs):
        from drunc.fsm.actions.utils import validate_run_type
        run_type = validate_run_type(run_type.upper())
        _input_data['production_vs_test'] = run_type

        _input_data["run"] = run_number
        _input_data['disable_data_storage'] = disable_data_storage
        _input_data['trigger_rate'] = trigger_rate
        return _input_data
