from drunc.fsm.fsm_core import FSMPlugin

class RunNumberPlugin(FSMPlugin):
    def __init__(self, configuration):
        super().__init__("run-number")
