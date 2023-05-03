from fsm_errors import *

class FSMInterfaceFactory:
    def __init__(self):
        pass

    def get(self, interface_name, configuration):
        match interface_name:
            case "run-number":
                from interfaces.RunNumberInterface import RunNumberInterface
                return RunNumberInterface(configuration) 
            case 'test-interface':
                from interfaces.TestInterface import TestInterface
                return TestInterface(configuration)
            case "logbook":
                from interfaces.LogbookInterface import LogbookInterface
                return LogbookInterface(configuration)
            case _:
                raise UnknownInterface(interface_name)

FSMInterfacesFact = FSMInterfaceFactory()