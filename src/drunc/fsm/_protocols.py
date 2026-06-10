from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Protocol

if TYPE_CHECKING:
    from druncschema.controller_pb2 import FSMSequence

    from drunc.fsm.core import PreOrPostTransitionSequence
    from drunc.fsm.transition import Transition


class ParameterProtocol(Protocol):
    name: str
    value: str


class InitConfigurationProtocol(Protocol):
    parameters: Iterable[ParameterProtocol]


class ActorProtocol(Protocol):
    def get_user_name(self) -> str: ...


class DetConfigProtocol(Protocol):
    id: str


class DALProtocol(Protocol):
    detector_configuration: DetConfigProtocol


class DBProtocol(Protocol):
    def get_dal(self, class_name: str, uid: str) -> DALProtocol: ...


class OksKeyProtocol(Protocol):
    session: str


class RuntimeConfigurationProtocol(Protocol):
    initial_data: str
    oks_key: OksKeyProtocol

    
class ConfigurationProtocol(Protocol):
    id: str
    db: DBProtocol
    oks_key: OksKeyProtocol
    initial_data: str
    parameters: Iterable[ParameterProtocol]
    name: str


class ContextProtocol(Protocol):
    actor: ActorProtocol
    configuration: ConfigurationProtocol
    runinfo: Dict[str, object]


class SessionDalProtocol(Protocol):
    segment: object
    rte_script: Optional[str]


class SSHCommandProtocol(Protocol):
    def __call__(self, *args: str, _err_to_out: bool = ...) -> object: ...


class ShErrorProtocol(Protocol):
    stdout: bytes
    stderr: bytes


class ActionMethodProtocol(Protocol):
    __name__: str
    __module__: str
    __self__: object
    def __call__(self, *args: object, **kwargs: object) -> object: ...


class FSMActionProtocol(Protocol):
    name: str
    

class ConfigProtocol(Protocol):
    def get_initial_state(self) -> str: ...
    def get_states(self) -> List[str]: ...
    def get_transitions(self) -> List[Transition]: ...
    def get_sequences(self) -> List[FSMSequence]: ...
    def get_pre_transitions_sequences(self) -> Dict[Transition, PreOrPostTransitionSequence]: ...
    def get_post_transitions_sequences(self) -> Dict[Transition, PreOrPostTransitionSequence]: ...


class ActionMethod(Protocol):
    def __call__(self, *args: object, **kwargs: object) -> object: ...
