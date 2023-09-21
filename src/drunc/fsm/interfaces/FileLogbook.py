from drunc.fsm.fsm_core import FSMInterface

class FileLogbook(FSMInterface):
    def __init__(self, configuration):
        super(FileLogbook, self).__init__(
            name = "file-logbook"
        )
        self.file = configuration['file_name']

    def post_start(self, _input_data, message:str="", **kwargs):

        with open(self.file, 'a') as f:
            f.write(f"Run {_input_data['run_num']} started")
            if message != "":
                f.write(message)

        return _input_data

    def post_stop(self, _input_data, **kwargs):

        with open(self.file, 'a') as f:
            f.write(f"Run {_input_data['run_num']} stopped")

        return _input_data