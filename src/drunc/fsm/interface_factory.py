import drunc.fsm.fsm_errors as fsm_errors
import inspect


class FSMInterfaceFactory:
    def __init__(self):
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('Call get() instead')

    def _get_pre_transitions(self, interface):
        retr = {}
        for name, method in inspect.getmembers(interface):
            if inspect.ismethod(method):
                if name.startswith('pre_'):
                    retr[name] = method
        return retr

    def _get_post_transitions(self, interface):
        retr = {}
        for name, method in inspect.getmembers(interface):
            if inspect.ismethod(method):
                if name.startswith('post_'):
                    retr[name] = method
        return retr

    def _validate_signature(self, name, method, interface):
        sig = inspect.signature(method)

        if 'kwargs' not in sig.parameters.keys() or '_input_data' not in sig.parameters.keys():
            raise fsm_errors.InvalidInterfaceMethod(interface, name)

        for pname, p in sig.parameters.items():
            if pname in ["_input_data", "args", "kwargs"]:
                continue

            if p.annotation is inspect._empty:
                raise fsm_errors.MethodSignatureMissingAnnotation(interface, name, pname)



    def _validate_interface(self, interface):
        pre_transition  = self._get_pre_transitions (interface)
        post_transition = self._get_post_transitions(interface)

        if not pre_transition and not post_transition:
            raise fsm_errors.InvalidInterface(interface.name)

        for k,v in pre_transition.items():
            self._validate_signature(k, v, interface.name)

        for k,v in post_transition.items():
            self._validate_signature(k, v, interface.name)

    def get_interface(self, interface_name, configuration):
        iface = None
        match interface_name:
            case "user-provided-run-number":
                from drunc.fsm.interfaces.UserProvidedRunNumber import UserProvidedRunNumber
                iface = UserProvidedRunNumber(configuration)
            case 'test-interface':
                from drunc.fsm.interfaces.TestInterface import TestInterface
                iface = TestInterface(configuration)
            case "file-logbook":
                from drunc.fsm.interfaces.FileLogbook import FileLogbook
                iface = FileLogbook(configuration)
            case _:
                raise fsm_errors.UnknownInterface(interface_name)

        self._validate_interface(iface)
        return iface


    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)

        return cls._instance
