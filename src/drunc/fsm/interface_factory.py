import drunc.fsm.fsm_errors

class FSMInterfaceFactory:
    def __init__(self):
        raise RuntimeError('Call get() instead')

    def get_interface(self, interface_name, configuration):
        match interface_name:
            case "run-number":
                from drunc.fsm.interfaces.DummyRunNumberInterface import RunNumberInterface
                return RunNumberInterface(configuration)
            case 'test-interface':
                from drunc.fsm.interfaces.TestInterface import TestInterface
                return TestInterface(configuration)
            case "logbook":
                from drunc.fsm.interfaces.DummyLogbookInterface import LogbookInterface
                return LogbookInterface(configuration)
            case _:
                raise fsm_errors.UnknownInterface(interface_name)
    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)

        return cls._instance
