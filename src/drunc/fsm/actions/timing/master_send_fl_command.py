from drunc.fsm.core import FSMAction
from drunc.fsm._protocols import ContextProtocol

class MasterSendFLCommand(FSMAction):
    def __init__(self, configuration: object) -> None:
        super().__init__(name="master-send-fl-command")

    def pre_master_send_fl_command(
        self,
        _input_data: dict[str, object],
        _context: ContextProtocol,
        fl_cmd_id: int,
        channel: int,
        number_of_commands_to_send: int,
        **kwargs: object,
    ) -> dict[str, object]:
        # parse fl_cmd_id...
        _input_data["fl_cmd_id"] = fl_cmd_id
        _input_data["channel"] = channel
        _input_data["number_of_commands_to_send"] = number_of_commands_to_send

        return _input_data
