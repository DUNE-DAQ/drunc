from drunc.fsm.core import FSMAction
from drunc.utils.utils import now_str

class SendMasterFLCommand(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "send-master-fl-command"
        )
        #self.conf_dict = {p.name: p.value for p in configuration.parameters}
        self.commands = {
            "time_sync_echo": 1,
            #...
        }


    def pre_send_master_fl_command(
        self,
        _input_data,
        _context,
        fl_cmd_id:str,
        channel:int,
        number_of_commands_to_send:int,
        **kwargs
    ):
        # parse fl_cmd_id...
        _input_data['fl_cmd_id'] = self.commands[fl_cmd_id]
        _input_data['channel'] = channel
        _input_data['number_of_commands_to_send'] = number_of_commands_to_send

        return _input_data
