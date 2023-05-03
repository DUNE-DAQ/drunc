from drunc.fsm.fsm_core import FSMInterface

class TestInterface(FSMInterface):
    def __init__(self, configuration):
        super().__init__("test-plugin")
        self.port = configuration['port']
        self.route = configuration['route']

    def pre_conf(self, data):
        print(f"Running pre_conf of {self.name}")
