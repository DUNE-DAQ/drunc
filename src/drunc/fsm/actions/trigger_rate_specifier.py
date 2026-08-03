from drunc.fsm._protocols import ConfigurationProtocol, ContextProtocol
from drunc.fsm.core import FSMAction


class TriggerRateSpecifier(FSMAction):
    def __init__(self, configuration: ConfigurationProtocol) -> None:
        super().__init__(name="trigger-rate-specifier")

    def pre_change_rate(
        self,
        _input_data: dict[str, object],
        _context: ContextProtocol,
        trigger_rate: float,
        **kwargs: object,
    ) -> dict[str, object]:
        _input_data["trigger_rate"] = trigger_rate
        return _input_data
