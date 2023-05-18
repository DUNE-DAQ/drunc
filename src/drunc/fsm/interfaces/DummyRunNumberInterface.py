from drunc.fsm.fsm_core import FSMInterface

class RunNumberInterface(FSMInterface):
    def __init__(self, configuration):
        super().__init__("run-number")

    def post_start(self, data):
        print(f"Running post-start of {self.name}")
        return {"run_num":22, "user":"fooUsr"}

