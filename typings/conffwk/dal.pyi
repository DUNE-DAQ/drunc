from typing import List

from drunc.fsm._protocols import (
    ConfigurationProtocol,
    DBProtocol,
    OksKeyProtocol,
    ParameterProtocol,
)

class FSMParameter(ParameterProtocol):
    name: str
    value: str

class FSMAction(ConfigurationProtocol):
    id: str
    name: str
    parameters: List[FSMParameter]
    db: DBProtocol
    oks_key: OksKeyProtocol
    initial_data: str

class FSMxTransition:
    transition: str
    order: List[str]
    mandatory: List[str]

class FSMTransitionConfig:
    id: str
    source: str
    dest: str

class FSMCommand:
    id: str

class FSMCommandSequence:
    id: str
    sequence: List[FSMCommand]

class FSMData:
    states: List[str]
    initial_state: str
    actions: List[FSMAction]
    transitions: List[FSMTransitionConfig]
    pre_transitions: List[FSMxTransition]
    post_transitions: List[FSMxTransition]
    command_sequences: List[FSMCommandSequence]
