import abc
from drunc.fsm.fsm_core import FSM

class StatefulNode(abc.ABC):
    def __init__(self, statefulnode_configuration, **kwargs):
        super(StatefulNode, self).__init__(
            **kwargs
        )
        self.fsm = FSM(statefulnode_configuration.get('fsm'))
        self.included = True
        self.in_error = False

    def include(self):
        self.included = True

    def exclude(self):
        self.included = False

    def is_included(self):
        return self.included

    def to_error(self):
        self.in_error = True

    def resolve_error(self):
        self.in_error = False

    def is_in_error(self):
        return self.in_error