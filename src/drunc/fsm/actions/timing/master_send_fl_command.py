from drunc.fsm.core import FSMAction
from drunc.utils.utils import now_str

class MasterSendFLCommand(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "master-send-fl-command"
        )

    def pre_master_send_fl_command(
        self,
        _input_data,
        _context,
        fl_cmd_id:int,
        channel:int,
        number_of_commands_to_send:int,
        **kwargs
    ):
        # parse fl_cmd_id...
        _input_data['fl_cmd_id'] = fl_cmd_id
        _input_data['channel'] = channel
        _input_data['number_of_commands_to_send'] = number_of_commands_to_send

        return _input_data
