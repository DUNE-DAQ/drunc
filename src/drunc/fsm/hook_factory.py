import drunc.fsm.exceptions as fsme
import inspect


class FSMHookFactory:
    def __init__(self):
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('Call get() instead')

    def _get_pre_transitions(self, hook):
        retr = {}
        for name, method in inspect.getmembers(hook):
            if inspect.ismethod(method):
                if name.startswith('pre_'):
                    retr[name] = method
        return retr

    def _get_post_transitions(self, hook):
        retr = {}
        for name, method in inspect.getmembers(hook):
            if inspect.ismethod(method):
                if name.startswith('post_'):
                    retr[name] = method
        return retr

    def _validate_signature(self, name, method, hook):
        sig = inspect.signature(method)

        if 'kwargs' not in sig.parameters.keys() or '_input_data' not in sig.parameters.keys():
            raise fsme.InvalidhookMethod(hook, name)

        for pname, p in sig.parameters.items():
            if pname in ["_input_data", "args", "kwargs"]:
                continue

            if p.annotation is inspect._empty:
                raise fsme.MethodSignatureMissingAnnotation(hook, name, pname)



    def _validate_hook(self, hook):
        pre_transition  = self._get_pre_transitions (hook)
        post_transition = self._get_post_transitions(hook)

        if not pre_transition and not post_transition:
            raise fsme.Invalidhook(hook.name)

        for k,v in pre_transition.items():
            self._validate_signature(k, v, hook.name)

        for k,v in post_transition.items():
            self._validate_signature(k, v, hook.name)

    def get_hook(self, hook_name, configuration):
        iface = None
        match hook_name:
            case "user-provided-run-number":
                from drunc.fsm.hooks.user_provided_run_number import UserProvidedRunNumber
                iface = UserProvidedRunNumber(configuration)
            case 'test-hook':
                from drunc.fsm.hooks.test_hook import Testhook
                iface = Testhook(configuration)
            case "file-logbook":
                from drunc.fsm.hooks.file_logbook import FileLogbook
                iface = FileLogbook(configuration)
            case _:
                raise fsme.Unknownhook(hook_name)

        self._validate_hook(iface)
        return iface


    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)

        return cls._instance
