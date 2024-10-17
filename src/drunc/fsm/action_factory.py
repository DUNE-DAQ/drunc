import drunc.fsm.exceptions as fsme
import inspect


class FSMActionFactory:
    def __init__(self):
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('Call get() instead')

    def _get_pre_transitions(self, action):
        retr = {}
        for name, method in inspect.getmembers(action):
            if inspect.ismethod(method):
                if name.startswith('pre_'):
                    retr[name] = method
        return retr

    def _get_post_transitions(self, action):
        retr = {}
        for name, method in inspect.getmembers(action):
            if inspect.ismethod(method):
                if name.startswith('post_'):
                    retr[name] = method
        return retr

    def _validate_signature(self, name, method, action):
        sig = inspect.signature(method)

        if 'kwargs' not in sig.parameters.keys() or '_input_data' not in sig.parameters.keys() or '_context' not in sig.parameters.keys():
            raise fsme.InvalidActionMethod(action, name)

        for pname, p in sig.parameters.items():
            if pname in ["_input_data", "_context", "args", "kwargs"]:
                continue

            if p.annotation is inspect._empty:
                raise fsme.MethodSignatureMissingAnnotation(action, name, pname)



    def _validate_action(self, action):
        pre_transition  = self._get_pre_transitions (action)
        post_transition = self._get_post_transitions(action)

        if not pre_transition and not post_transition:
            raise fsme.InvalidAction(action.name)

        for k,v in pre_transition.items():
            self._validate_signature(k, v, action.name)

        for k,v in post_transition.items():
            self._validate_signature(k, v, action.name)

    def get_action(self, action_name, configuration):
        iface = None
        match action_name:
            case "user-provided-run-number":
                from drunc.fsm.actions.user_provided_run_number import UserProvidedRunNumber
                iface = UserProvidedRunNumber(configuration)
            case "usvc-provided-run-number":
                from drunc.fsm.actions.usvc_provided_run_number import UsvcProvidedRunNumber
                iface = UsvcProvidedRunNumber(configuration)
            case 'test-action':
                from drunc.fsm.actions.test_action import TestAction
                iface = TestAction(configuration)
            case "file-logbook":
                from drunc.fsm.actions.file_logbook import FileLogbook
                iface = FileLogbook(configuration)
            case "elisa-logbook":
                from drunc.fsm.actions.usvc_elisa_logbook import ElisaLogbook
                iface = ElisaLogbook(configuration)
            case "file-run-registry":
                from drunc.fsm.actions.file_run_registry import FileRunRegistry
                iface = FileRunRegistry(configuration)
            case "db-run-registry":
                from drunc.fsm.actions.db_run_registry import DBRunRegistry
                iface = DBRunRegistry(configuration)
            case "thread-pinning":
                from drunc.fsm.actions.thread_pinning import ThreadPinning
                iface = ThreadPinning(configuration)
            case "master-send-fl-command":
                from drunc.fsm.actions.timing.master_send_fl_command import MasterSendFLCommand
                iface = MasterSendFLCommand(configuration)
            case "trigger-rate-specifier":
                from drunc.fsm.actions.trigger_rate_specifier import TriggerRateSpecifier
                iface = TriggerRateSpecifier(configuration)
            case _:
                raise fsme.UnknownAction(action_name)

        self._validate_action(iface)
        return iface


    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)

        return cls._instance
