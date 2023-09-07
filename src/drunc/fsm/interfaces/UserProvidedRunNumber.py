from drunc.fsm.fsm_core import FSMInterface

class UserProvidedRunNumber(FSMInterface):
    def __init__(self, configuration):
        super(UserProvidedRunNumber, self).__init__(
            name = "run-number"
        )

    def post_start(self, _input_data:dict, run_number:int=1, **kwargs):
        _input_data["run_num"] = run_number
        return _input_data
