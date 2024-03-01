from drunc.utils.configuration import ConfHandler, ConfTypes, OKSKey
from drunc.fsm.fsm_core import PreOrPostTransitionSequence

class FSMConfHandler(ConfHandler):
    def _fill_pre_post_transition_sequence_oks(self, prefix, transition, data):
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

    def _post_process_oks(self):
        self.log.info('_post_process_oks configuration')
        self.pre_transitions  = {}
        self.post_transitions = {}
        self.interfaces = {}
        self.transitions = []
        self.states = self.data.states
        self.initial_state = self.data.initial_state

        from drunc.fsm.interface_factory import FSMInterfaceFactory

        for interface in self.data.interfaces:
            self.log.info(f'Setting up interface \'{interface.id}\'')
            self.interfaces[interface.id] = FSMInterfaceFactory.get().get_interface(
                interface.id,
                interface
            )


        from drunc.fsm.transition import Transition

        for transition in self.data.transitions:
            tr = Transition(
                name = transition.id,
                source = transition.source,
                destination = transition.dest,
                arguments = [] # not needed in principle, but I getting transition from the previous iteration I don't add this (?!?!)
            )

            pre_transitions  = self._fill_pre_post_transition_sequence_oks('pre' , tr, self.data.pre_transitions)
            post_transitions = self._fill_pre_post_transition_sequence_oks('post', tr, self.data.post_transitions)

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
        return self.data.initial_state

    def get_states(self):
        return self.data.states

    def get_transitions(self):
        return self.transitions

    def get_pre_transitions_sequences(self):
        return self.pre_transitions

    def get_post_transitions_sequences(self):
        return self.post_transitions

