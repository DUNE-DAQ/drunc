from drunc.fsm.fsm_core import FSMPlugin

class TestPlugin(FSMPlugin):
    def __init__(self, configuration):
        super().__init__("test-plugin")
        self.port = configuration['port']
        self.route = configuration['route']
