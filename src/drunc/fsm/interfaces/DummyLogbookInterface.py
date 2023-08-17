from drunc.fsm.fsm_core import FSMInterface

class LogbookInterface(FSMInterface):
    def __init__(self, configuration):
        super().__init__("logbook")

    def post_start(self, data, message=""):
        print(f"Running post_start of {self.name}")
        print(f"Run {data['run_num']} started by {data['user']}.")
        if message != "":
            print(f"{data['user']}: {message}")

    def post_stop(self, data):
        print(f"Running post_stop of {self.name}")