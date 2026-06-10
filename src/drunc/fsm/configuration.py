import logging
from typing import Dict, List

from druncschema.controller_pb2 import FSMSequence

from drunc.fsm._protocols import ConfigurationProtocol
from drunc.fsm.action_factory import FSMActionFactory, FSMActionProtocol
from drunc.fsm.core import PreOrPostTransitionSequence
from drunc.fsm.transition import Transition
from drunc.utils.configuration import ConfHandler, OKSKey
from drunc.utils.utils import get_logger


class FSMConfHandler(ConfHandler):

    def __init__(self, key: OKSKey) -> None:
        super().__init__(key)
        self.log: logging.Logger = get_logger("controller.core.FSMConfHandler")
        self.pre_transitions: Dict[Transition, PreOrPostTransitionSequence] = {}
        self.post_transitions: Dict[Transition, PreOrPostTransitionSequence] = {}
        self.actions: Dict[str, FSMActionProtocol] = {}   # (e.g. "thread_pinning": thread_pinning FSM action object)
        self.transitions: List[Transition] = []
        self.sequences: List[FSMSequence] = []
        self.states: List[str] = []
        self.initial_state: str = ""
        
    def _fill_pre_post_transition_sequence_oks(
        self,
        prefix: str,
        transition: Transition,
        data: List[ConfigurationProtocol] | None,
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
            if getattr(fsm_x_transition, "transition", None) == transition.name:
                for action_name in getattr(fsm_x_transition, "order", []):
                    mandatory_list = getattr(fsm_x_transition, "mandatory", [])
                    seq.add_callback(
                        action=self.actions[action_name],
                        mandatory=action_name in mandatory_list,
                    )
                break
        return seq

    def _post_process_oks(self) -> None:
        """
        Post-process the configuration data after it has been loaded and validated.

        This method:
            - initializes the FSM configuration
            - fills the pre and post transition sequences for each transition
            - adds the arguments of the pre and post transition sequences to the
                corresponding transition

        Args:
            None

        Returns:
            None

        Raises:
            None
        """

        # Fill the actions dictionary with the FSMAction objects corresponding to the
        # action names defined in the configuration
        for action in self.data.actions: 
            self.actions[action.id] = FSMActionFactory.get().get_action(
                action.id, action
            )

        for transition in self.data.transitions: 
            tr = Transition(
                name=transition.id,
                source=transition.source,
                destination=transition.dest,
                arguments=[],  # list(google.protobuf.any_pb2.Any)
            )

            # Get the pre and post transition sequences for the transition
            pre_transitions: PreOrPostTransitionSequence = (
                self._fill_pre_post_transition_sequence_oks(
                    "pre", tr, self.data.pre_transitions
                )
            )
            post_transitions: PreOrPostTransitionSequence = (
                self._fill_pre_post_transition_sequence_oks(
                    "post", tr, self.data.post_transitions
                )
            )

            if tr.arguments is not None:
                # Add the pre and post transition sequence arguments to the transition
                tr.arguments += pre_transitions.get_arguments()
                tr.arguments += post_transitions.get_arguments()

            # Store the pre and post transition sequences for the transition
            self.pre_transitions[tr] = pre_transitions
            self.post_transitions[tr] = post_transitions

            # Add the transition to the list of transitions
            self.transitions += [tr]

        # Fill the sequences of commands to execute for each transition
        for sequence in self.data.command_sequences:
            seq_id = sequence.id
            cmd_ids = [cmd.id for cmd in sequence.sequence]
            self.sequences.append(FSMSequence(id=seq_id, command_ids=cmd_ids))

    def get_actions(self) -> Dict[str, FSMActionProtocol]:
        return self.actions

    def get_initial_state(self) -> str:
        return str(self.data.initial_state)

    def get_states(self) -> List[str]:
        return list(self.data.states)

    def get_transitions(self) -> List[Transition]:
        return self.transitions

    def get_pre_transitions_sequences(self) -> Dict[Transition, PreOrPostTransitionSequence]:
        return self.pre_transitions

    def get_post_transitions_sequences(self) -> Dict[Transition, PreOrPostTransitionSequence]:
        return self.post_transitions

    def get_sequences(self) -> List[FSMSequence]    :
        return self.sequences
