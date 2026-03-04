from druncschema.controller_pb2 import FSMSequence

from drunc.fsm.action_factory import FSMActionFactory
from drunc.fsm.core import PreOrPostTransitionSequence
from drunc.fsm.transition import Transition
from drunc.utils.configuration import ConfHandler
from drunc.utils.utils import get_logger


class FSMConfHandler(ConfHandler):
    def _fill_pre_post_transition_sequence_oks(
        self,
        prefix: str,
        transition: Transition,
        data: list("conffwk.dal.FSMxTransition"),
    ) -> PreOrPostTransitionSequence:
        """
        Fill the pre or post transition sequence for a given transition.

        Args:
            prefix (str): "pre" or "post" to indicate which sequence to fill.
            transition (Transition): The transition for which to fill the sequence.
            data: The data containing all configuration pre or post transition sequences.

        Returns:
            PreOrPostTransitionSequence: The filled pre or post transition sequence for the given transition.

        Raises:
            ValueError: If the prefix is not "pre" or "post".
        """
        self.log = get_logger("controller.core.FSMConfHandler")
        seq = PreOrPostTransitionSequence(
            transition,
            prefix,
        )

        if data is None:
            return seq

        # Iterate through the FSMxTransitions to find the one matching the current
        # transition. There is one FSMxTransition per transition. The FSMxTransition
        # contains the list of actions to execute for the given transition.
        for fsm_x_transition in data:
            if fsm_x_transition.transition == transition.name:
                # print(f"Found matching FSMxTransition: {fsm_x_transition}")
                for action_name in fsm_x_transition.order:
                    seq.add_callback(
                        action=self.actions[action_name],
                        mandatory=action_name in fsm_x_transition.mandatory,
                    )
                break
        return seq

    def _post_process_oks(self):
        self.log.debug("_post_process_oks configuration")
        self.pre_transitions: dict(Transition, PreOrPostTransitionSequence) = {}
        self.post_transitions: dict(Transition, PreOrPostTransitionSequence) = {}
        self.actions: dict(
            str, "conffwk.dal.FSMAction"
        ) = {}  # (e.g. "thread_pinning": thread_pinning FSM action object)
        self.transitions: list(Transition) = []  # list of Transition
        self.sequences = []  # list of FSMSequence
        self.states = self.data.states
        self.initial_state = self.data.initial_state

        for action in self.data.actions:
            # self.log.critical(f"Setting up action '{action.id}' with action data: {action} (of type {type(action)})")
            # Setting up action 'file-run-registry' with action data: FSMaction(id='file-run-registry',
            #     commands = [],
            #     name = file-run-registry,
            #     parameters = []]) (of type <class 'conffwk.dal.FSMaction'>)
            self.actions[action.id] = FSMActionFactory.get().get_action(
                action.id, action
            )

        self.log.critical(f"self.data.transitions: {self.data.transitions}")
        for transition in self.data.transitions:
            tr = Transition(
                name=transition.id,
                source=transition.source,
                destination=transition.dest,
                arguments=[],  # /!\
            )
            print("")
            print(f"Transition: {tr.name}")
            # print(f"Pre transitions?: {self.data.pre_transitions}")
            # print(f"Post transitions?: {self.data.post_transitions}")
            # print("Calling fun pre-transitions")
            pre_transitions: PreOrPostTransitionSequence = (
                self._fill_pre_post_transition_sequence_oks(
                    "pre", tr, self.data.pre_transitions
                )
            )
            print(f"\t\tGot pre transitions: {pre_transitions}")
            print(f"\t\tGot pre transitions args: {pre_transitions.get_arguments()}")
            # print("Calling fun post-transitions")
            post_transitions: PreOrPostTransitionSequence = (
                self._fill_pre_post_transition_sequence_oks(
                    "post", tr, self.data.post_transitions
                )
            )
            # print(f"Got post transitions: {post_transitions}")

            # print(f"Before adding args: {tr.arguments}")
            tr.arguments += pre_transitions.get_arguments()
            tr.arguments += post_transitions.get_arguments()
            # print(f"After adding args: {tr.arguments}")

            # print("Sanity check")
            print(f"{pre_transitions=}")
            print(f"{pre_transitions.get_arguments()=}")

            self.pre_transitions[tr] = pre_transitions
            self.post_transitions[tr] = post_transitions

            # print(f"Final transition: {tr}")
            # print(f"Final args: {tr.arguments}")

            self.transitions += [tr]

        for sequence in self.data.command_sequences:
            seq_id = sequence.id
            cmd_ids = [cmd.id for cmd in sequence.sequence]
            self.sequences.append(FSMSequence(id=seq_id, command_ids=cmd_ids))

        # import pprint
        # print("")
        # pprint.pprint(self.actions, indent=4)

    # def _parse_dict(self, data):
    #     pass

    def get_actions(self):
        return self.actions

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

    def get_sequences(self):
        return self.sequences
