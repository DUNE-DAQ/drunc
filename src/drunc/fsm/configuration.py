from drunc.utils.configuration_utils import ConfigurationHandler, ConfTypes
from drunc.exceptions import DruncSetupException


class FSMConfiguration(ConfigurationHandler):
    def __init__(self, configuration, uid):
        super().__init__(configuration)
        self.uid = uid
        if self.conf.type not in [ConfTypes.PyObject, ConfTypes.OKSObject]:
            raise DruncSetupException('FSM Configuration was not parsed correctly')

        self.transitions = []

        from drunc.fsm.transition import Transition

        match self.conf.type:
            case ConfTypes.OKSObject:
                for transition in self.conf.data.transitions:
                    tr = Transition(
                        name = transition.id,
                        source = transition.source,
                        destination = transition.dest,
                    )
                    self.transitions += [tr]

            case ConfTypes.DAQConfDir:
                pass




    def _parse_dict(self, data):
        pass

    def get_initial_state(self):
        return self.conf.data.initial_state

    def get_states(self):
        return self.conf.data.states

    def get_transitions(self):
        return self.transitions

    def get_pre_transitions_sequences(self):
        pass

    def get_post_transitions_sequences(self):
        pass

