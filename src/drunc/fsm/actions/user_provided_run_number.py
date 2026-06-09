import time
from typing import Optional

from drunc.fsm.actions.utils import validate_run_type
from drunc.fsm.core import FSMAction


class UserProvidedRunNumber(FSMAction):
    def __init__(self, configuration: object) -> None:
        super().__init__(name="run-number")

    def pre_start(
        self,
        _input_data: dict[str, object],
        _context: object,
        run_number: int,
        run_type: Optional[str] = "TEST",
        disable_data_storage: bool = False,
        trigger_rate: Optional[float] = None,
        **kwargs: object,
    ) -> dict[str, object]:
        
        safe_run_type = run_type.upper() if run_type is not None else "TEST"
        run_type = validate_run_type(safe_run_type)
        _input_data["production_vs_test"] = run_type
        _input_data["run"] = run_number
        _input_data["disable_data_storage"] = disable_data_storage
        if trigger_rate is not None:
            _input_data["trigger_rate"] = trigger_rate

        _input_data["run_time_at_start"] = time.time()

        return _input_data
