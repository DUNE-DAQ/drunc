import abc
from drunc.fsm.fsm_core import FSM
from drunc.broadcast.server.broadcast_sender import BroadcastSender

class Observed:
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._broadcast_on_change.broadcast(
            message = f'Changing {self._name} from {self._value} to {value}',
            btype = self._broadcast_key,
        )
        self._value = value

    def __init__(self, name, broadcast_on_change:BroadcastSender, broadcast_key, initial_value=None):
        self._name = name
        self._broadcast_on_change = broadcast_on_change
        self._value = initial_value
        self._broadcast_key = broadcast_key


class FSMState(Observed):
    def __init__(self, **kwargs):
        super(FSMState, self).__init__(
            name = 'fsm_state',
            **kwargs
        )


class ErrorState(Observed):
    def __init__(self, **kwargs):
        super(ErrorState, self).__init__(
            name = 'error_state',
            **kwargs
        )


class InclusionState(Observed):
    def __init__(self, **kwargs):
        super(InclusionState, self).__init__(
            name = 'inclusion_state',
            **kwargs
        )

class StatefulNodeException(Exception):
    pass

class CannotInclude(Exception):
    pass

class CannotExclude(Exception):
    pass


class StatefulNode(abc.ABC, BroadcastSender):
    def __init__(self, statefulnode_configuration, **kwargs):
        super(StatefulNode, self).__init__(
            **kwargs
        )
        self.fsm = FSM(statefulnode_configuration.get('fsm'))

        from druncschema.broadcast_pb2 import BroadcastType
        self.state = FSMState(
            broadcast_on_change = self,
            broadcast_key = BroadcastType.FSM_STATUS_UPDATE,
            initial_value = self.fsm.current_state
        )
        self.included = InclusionState(
            broadcast_on_change = self,
            broadcast_key = BroadcastType.STATUS_UPDATE,
            initial_value = True
        )
        self.in_error = ErrorState(
            broadcast_on_change = self,
            broadcast_key = BroadcastType.STATUS_UPDATE,
            initial_value = False
        )

    def node_fsm_state(self):
        return self.state.value

    def include_node(self):
        if self.included.value:
            raise CannotInclude()
        self.included.value = True

    def exclude_node(self):
        if not self.included.value:
            raise CannotExclude()
        self.included.value = False

    def node_is_included(self):
        return self.included.value

    def to_error(self):
        self.in_error.value = True

    def resolve_error(self):
        self.in_error.value = False

    def node_is_in_error(self):
        return self.in_error.value