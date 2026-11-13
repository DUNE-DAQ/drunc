from google.rpc import code_pb2

from drunc.exceptions import DruncNonTerminalException


class FSMTransitionException(DruncNonTerminalException):
    pass


class NoTransitionOfName(FSMTransitionException):
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "UNKNOWN_FSM_TRANSITION"

    def __init__(self, transition_name: str, **kwargs) -> None:
        self.message = f'Transition "{transition_name}" does not exist'
        super().__init__(self.message, transition_name=transition_name, **kwargs)  # adds to error metadata


class DuplicateTransition(FSMTransitionException):
    """If a transition has the same name as another"""
    grpc_error_code = code_pb2.ALREADY_EXISTS
    reason = "DUPLICATE_FSM_TRANSITION"

    def __init__(self, transition_name: str, **kwargs) -> None:
        self.message = f'Transition "{transition_name}" is a duplicate'
        super().__init__(self.message, transition_name=transition_name,**kwargs)


class InvalidTransition(FSMTransitionException):
    """Raised when the transition isn't in the list of currently accessible transitions"""
    grpc_error_code = code_pb2.NOT_FOUND
    reason = "INVALID_FSM_TRANSITION"

    def __init__(self, transition: str, state: str, **kwargs) -> None:
        self.message = f"Transition {transition} is not allowed from the state {state}."
        super().__init__(self.message, transition=transition, state=state, **kwargs)  # adds to error metadata


class UnregisteredTransition(FSMTransitionException):
    """Raised when the transition is allowed, but we can't find the implementation"""
    grpc_error_code = code_pb2.NOT_FOUND
    reason = "UNREGISTERED_FSM_TRANSITION"

    def __init__(self, transition: str, **kwargs) -> None:
        self.message = f"Implementation of {transition} not found."
        super().__init__(self.message, transition=transition, **kwargs)  


class UnknownAction(FSMTransitionException):
    """Raised when a plugin name is provided that does not correspond to any files in /plugins"""
    grpc_error_code = code_pb2.NOT_FOUND
    reason = "UNKNOWN_FSM_ACTION"


    def __init__(self, name: str, **kwargs) -> None:
        self.message = f'"{name}" is not a known plugin.'
        super().__init__(self.message, name=name, **kwargs)


class MissingArgument(FSMTransitionException):
    """Raised when a mandatory argument is not provided for a transition"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "MISSING_FSM_ARGUMENT"

    def __init__(self, param: str, name: str, **kwargs) -> None:
        self.message = f'The mandatory argument "{param}" was not provided to the transition {name}'
        super().__init__(self.message, param=param, name=name, **kwargs)  


class MissingArgumentValue(FSMTransitionException):
    """Raised when a mandatory argument is not provided for a transition"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "MISSING_FSM_ARGUMENT_VALUE"

    def __init__(self) -> None:
        self.message = "A passed argument does not have an associated value, arguments are key-value pairs."
        super().__init__(self.message)


class DoubleArgument(FSMTransitionException):
    """Raised when an argument is provided more than once"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "DOUBLE_FSM_ARGUMENT"  

    def __init__(self, txt: str) -> None:
        self.message = txt
        super().__init__(self.message)


class UnhandledArgumentType(FSMTransitionException):
    """Raised when an argument has an unsupported type"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "UNHANDLED_FSM_ARGUMENT_TYPE"
    
    def __init__(self, annotation: str, **kwargs) -> None:
        self.message = f'The argument "{annotation}" cannot be handled'
        super().__init__(self.message, annotation=annotation, **kwargs)


class UnknownArgument(FSMTransitionException):
    """Raised when an unwanted argument is given to a transition"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "UNKNOWN_FSM_ARGUMENT"

    def __init__(self, param: str, name: str, **kwargs) -> None:
        self.message = (
            f'The mandatory argument "{param}" is not required by transition {name}'
        )
        super().__init__(self.message, param=param, name=name, **kwargs)


class InvalidAction(FSMTransitionException):
    """Raised when an action doesn't have pre/post transitions"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "INVALID_FSM_ACTION"

    def __init__(self, iface: str, **kwargs) -> None:
        self.message = (
            f'The action "{iface}" does not have any pre or post transition method'
        )
        super().__init__(self.message, iface=iface, **kwargs)


class InvalidActionMethod(FSMTransitionException):
    """Raised when an action doesn't have the pre/post transitions arguments"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "INVALID_FSM_ACTION_METHOD"

    def __init__(self, iface: str, method: str, **kwargs) -> None:
        self.message = f'The action "{iface}" method {method} does not have the correct arguements, each one should have at least "_input_data" and "**kwargs", and have type annotations'
        super().__init__(self.message, iface=iface, method=method, **kwargs)


class MethodSignatureMissingAnnotation(FSMTransitionException):
    """Raised when the action does not have the correct arguments"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "METHOD_SIGNATURE_MISSING_ANNOTATION"

    def __init__(self, iface: str, method: str, pname: str, **kwargs) -> None:
        self.message = f'The action "{iface}" method {method} does not have the correct argument annotation for "{pname}", provide "argument:int" to your pre/post methods.'
        super().__init__(self.message, iface=iface, method=method, pname=pname, **kwargs)


class TransitionDataOfIncorrectFormat(FSMTransitionException):
    """Raised when the transition data is of incorrect format"""
    grpc_error_code = code_pb2.INVALID_ARGUMENT
    reason = "TRANSITION_DATA_INCORRECT_FORMAT"
    def __init__(self, data: object, **kwargs) -> None:
        self.message = f'The data "{data}" could not be interpreted as json'
        super().__init__(self.message, data=data, **kwargs)


class CannotGetRunNumber(FSMTransitionException):
    """Raised when the FSM cannot get the run number"""
    grpc_error_code = code_pb2.UNAVAILABLE
    reason = "FSM_CANNOT_GET_RUN_NUMBER"
    def __init__(self, data: object, **kwargs) -> None:
        self.message = f"Could not get Run Number because {data}"
        super().__init__(self.message, data=data, **kwargs)


class DBRunRegistryConfigurationError(FSMTransitionException):
    """Raised when there is a configuration error in the RunRegistryDB"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "DB_RUN_REGISTRY_CONFIGURATION_ERROR"
    def __init__(self, data: object, **kwargs) -> None:
        self.message = f"RunRegistryDB configuration error: {data}"
        super().__init__(self.message, data=data, **kwargs)


class CannotInsertRunNumber(FSMTransitionException):
    """Raised when the FSM cannot insert the run number into the RunRegistryDB"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "CANNOT_INSERT_RUN_NUMBER"

    def __init__(self, data: object, **kwargs) -> None:
        self.message = f"Could not insert Run into RunRegistryDB because {data}"
        super().__init__(self.message, data=data, **kwargs)


class CannotUpdateStopTime(FSMTransitionException):
    """Raised when the FSM cannot update the stop time in the RunRegistryDB"""
    grpc_error_code = code_pb2.UNAVAILABLE
    reason = "CANNOT_UPDATE_STOP_TIME"

    def __init__(self, data: object, **kwargs) -> None:
        self.message = f"Could not update stop time in RunRegistryDB because {data}"
        super().__init__(self.message, data=data, **kwargs)


class InvalidDataReturnByFSMAction(FSMTransitionException):
    """Raised when the FSM action returns invalid data"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "INVALID_DATA_RETURN_BY_FSM_ACTION"

    def __init__(self, data: object, **kwargs) -> None:
        self.message = (
            f"The action returns an incorrect object which isn't serialisable: {data}"
        )
        super().__init__(self.message, data=data, **kwargs)


class ThreadPinningFailed(FSMTransitionException):
    """Raised when thread pinning fails"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "THREAD_PINNING_FAILED"

    def __init__(self, host: str, **kwargs) -> None:
        self.message = f'The thread pinning on "{host}" failed'
        super().__init__(self.message, host=host, **kwargs)


class CannotGetSoftwareVersion(FSMTransitionException):
    """Raised when the software version cannot be retrieved from the environment"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "CANNOT_GET_SOFTWARE_VERSION"
    
    def __init__(self, **kwargs) -> None:
        self.message = "RunRegistryDB: dunedaq version not in the variable env DUNE_DAQ_BASE_RELEASE! Exit drunc and export DUNE_DAQ_BASE_RELEASE=dunedaq-vX.XX.XX\n"
        super().__init__(self.message,**kwargs)


class CannotSendElisaMessage(FSMTransitionException):
    """Raised when sending a message to ELisA fails"""
    grpc_error_code = code_pb2.UNAVAILABLE
    reason = "CANNOT_SEND_ELISA_MESSAGE"

    def __init__(self, data: object, **kwargs) -> None:
        self.message = f"Cannot send message to ELisA because {data}. Do it manually at https://np-vd-coldbox-elog.app.cern.ch or https://pdsp-elog.app.cern.ch!"
        super().__init__(self.message, data=data, **kwargs)


class DotDruncJsonNotFound(FSMTransitionException):
    """Raised when the .drunc.json file is not found"""
    grpc_error_code = code_pb2.NOT_FOUND
    reason = "DOT_DRUNC_JSON_NOT_FOUND"

    pass


class DotDruncJsonIncorrectFormat(FSMTransitionException):
    """Raised when the .drunc.json file has an incorrect format"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "DOT_DRUNC_JSON_INCORRECT_FORMAT"

    pass


class InvalidRunType(FSMTransitionException):
    """Raised when the run type is invalid"""
    grpc_error_code = code_pb2.INTERNAL
    reason = "INVALID_RUN_TYPE"

    pass
