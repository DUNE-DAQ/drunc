from drunc.utils.configuration_utils import ConfigurationHandler, ConfTypes
from drunc.exceptions import DruncSetupException
from drunc.fsm.fsm_core import PreOrPostTransitionSequence

class FSMConfiguration(ConfigurationHandler):
    def __init__(self, configuration):

        super().__init__(configuration)

        if self.conf.type not in [ConfTypes.PyObject, ConfTypes.OKSObject]:
            raise DruncSetupException('FSM Configuration was not parsed correctly')

        self.transitions = []
        self.states = self.conf.data.states
        self.initial_state = self.conf.data.initial_state
        self.pre_transitions  = {}
        self.post_transitions = {}
        self.interfaces = {}

        from drunc.fsm.interface_factory import FSMInterfaceFactory
        from drunc.utils.configuration_utils import ConfData

        for interface in self.conf.data.interfaces:
            self.interfaces[interface.id] = FSMInterfaceFactory.get().get_interface(
                ConfData(self.conf.type, interface)
            )

        from drunc.fsm.transition import Transition


        for transition in self.conf.data.transitions:
            tr = Transition(
                name = transition.id,
                source = transition.source,
                destination = transition.dest,
            )

            for prefix in ['pre', 'post']:
                seq = PreOrPostTransitionSequence(
                    tr,
                    prefix,
                )

                data = getattr(self.conf.data, f'{prefix}_transitions', None)

                if not data:
                    if prefix == 'pre':
                        self.pre_transitions[tr] = seq
                    else:
                        self.post_transitions[tr] = seq
                    continue

                class empty_sequence_conf_data:
                    def __init__(self):
                        self.order = []
                        self.mandatory = []
                empty_seq = empty_sequence_conf_data()

                blurp = getattr(data, tr.name, empty_seq)

                for interface in blurp.order:
                    seq.add_callback(
                        interface = self.interfaces[interface],
                        mandatory = interface in blurp.get("mandatory", []),
                    )
                tr.arguments += seq.get_arguments()

                if prefix == 'pre':
                    self.pre_transitions[tr] = seq
                else:
                    self.post_transitions[tr] = seq

            self.transitions += [tr]

    def _parse_dict(self, data):
        pass

    def get_interfaces(self):
        return self.interfaces

    def get_initial_state(self):
        return self.initial_state

    def get_states(self):
        return self.states

    def get_transitions(self):
        return self.transitions

    def get_pre_transitions_sequences(self):
        return self.pre_transitions

    def get_post_transitions_sequences(self):
        return self.post_transitions

