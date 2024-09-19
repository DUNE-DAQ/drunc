from drunc.fsm.core import FSMAction
from drunc.utils.utils import now_str

class FileLogbook(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "file-logbook"
        )
        self.conf_dict = {p.name: p.value for p in configuration.parameters}
        self.file = self.conf_dict['file_name']

    def post_start(self, _input_data, _context, file_logbook_post:str="", **kwargs):
        with open(self.file, 'a') as f:
            f.write(f"Run {_input_data['run']} started by {_context.actor.get_user_name()} at {now_str()}\n")
            if  file_logbook_post != "":
                f.write(file_logbook_post)
                f.write("\n")

        return _input_data

    def post_drain_dataflow(self, _input_data, _context, file_logbook_post:str="", **kwargs):
        with open(self.file, 'a') as f:
            f.write(f"Current run stopped by {_context.actor.get_user_name()} at {now_str()}\n")
            if file_logbook_post != "":
                f.write(file_logbook_post)
                f.write("\n")

        return _input_data
