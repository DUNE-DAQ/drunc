from drunc.utils.configuration_utils import ConfigurationHandler, ConfTypes
from drunc.exceptions import DruncSetupException
from drunc.fsm.fsm_core import PreOrPostTransitionSequence

class FSMConfiguration(ConfigurationHandler):
    def __init__(self, configuration):
        self.transitions = []
        self.states = []
        self.initial_state = ''
        self.pre_transitions  = {}
        self.post_transitions = {}
        self.interfaces = {}
        super().__init__(configuration)

        if self.conf.type == ConfTypes.OKSObject:
            self.__configure_with_oks(self.conf.data)



    def __fill_pre_post_transition_sequence_oks(self, prefix, transition, data):
        seq = PreOrPostTransitionSequence(
            transition,
            prefix,
        )

        if data is None:
            return seq

        class empty_sequence_conf_data:
            order = []
            mandatory = []

        seq_conf = empty_sequence_conf_data()

        for fsm_x_transition in data:
            if fsm_x_transition.id == transition.name:
                seq_conf = fsm_x_transition

        for interface in seq_conf.order:
            seq.add_callback(
                interface = self.interfaces[interface],
                mandatory = interface in seq_conf.mandatory,
            )


        return seq

    def __configure_with_oks(self, data):
        self.states = data.states
        self.initial_state = data.initial_state

        from drunc.fsm.interface_factory import FSMInterfaceFactory
        from drunc.utils.configuration_utils import ConfData, ConfTypes

        for interface in data.interfaces:
            self.log.info(f'Setting up interface \'{interface.id}\'')
            self.interfaces[interface.id] = FSMInterfaceFactory.get().get_interface(
                interface.id,
                ConfData(ConfTypes.OKSObject, interface)
            )


        from drunc.fsm.transition import Transition

        for transition in data.transitions:
            tr = Transition(
                name = transition.id,
                source = transition.source,
                destination = transition.dest,
                arguments = [] # not needed in principle, but I getting transition from the previous iteration I don't add this (?!?!)
            )

            pre_transitions  = self.__fill_pre_post_transition_sequence_oks('pre' , tr, data.pre_transitions)
            post_transitions = self.__fill_pre_post_transition_sequence_oks('post', tr, data.post_transitions)

            tr.arguments += pre_transitions .get_arguments()
            tr.arguments += post_transitions.get_arguments()

            self.pre_transitions [tr] = pre_transitions
            self.post_transitions[tr] = post_transitions

            self.transitions += [tr]

    # def _parse_dict(self, data):
    #     pass

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

