from fsm_errors import *


class FSMInterfaceFactory:
    def __init__(self):
        pass

    def get(self, interface_name, configuration):
        '''
        match interface_name:
            case "run_number"
                return RunNumberPlugin(configuration)               #Must be Defined in /plugins
        '''
        if interface_name == "run-number":
            from plugins.RunNumberPlugin import RunNumberPlugin
            return RunNumberPlugin(configuration) 
        elif interface_name == 'test-plugin':
            from plugins.TestPlugin import TestPlugin
            return TestPlugin(configuration)
        elif interface_name == "logbook":
            from plugins.TestPlugin import TestPlugin
            return TestPlugin(configuration)
        else:
            raise UnknownPlugin(interface_name)


FSMInterfacesFact = FSMInterfaceFactory()