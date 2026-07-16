from __future__ import annotations

from typing import List

import conffwk
from druncschema.controller_pb2 import FSMSequence

from drunc.fsm._protocols import FSMActionProtocol
from drunc.fsm.action_factory import FSMActionFactory
from drunc.fsm.core import PreOrPostTransitionSequence
from drunc.fsm.transition import Transition
from drunc.utils.configuration import ConfHandler
from drunc.utils.utils import get_logger


class FSMConfHandler(ConfHandler):
    """Handler for FSM configuration."""

    data: conffwk.dal.FSMData

    def _fill_pre_post_transition_sequence_oks(
        self,
        prefix: str,
        transition: Transition,
        data: List[conffwk.dal.FSMxTransition] | None,
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
                for action_name in fsm_x_transition.order:
                    seq.add_callback(
                        action=self.actions[action_name],
                        mandatory=action_name in fsm_x_transition.mandatory,
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
        raw = self._raw_data

        # Define the data structures to store the FSM configuration
        self.log.debug("_post_process_oks configuration")
        self.pre_transitions: dict[Transition, PreOrPostTransitionSequence] = {}
        self.post_transitions: dict[Transition, PreOrPostTransitionSequence] = {}
        self.actions: dict[
            str, FSMActionProtocol
        ] = {}  # (e.g. "thread_pinning": thread_pinning FSM action object)
        self.transitions: list[Transition] = []
        self.sequences: list[FSMSequence] = []
        self.states: list[str] = raw.states
        self.initial_state: str = raw.initial_state

        # Fill the actions dictionary with the FSMAction objects corresponding to the
        # action names defined in the configuration
        for action in raw.actions:  # type: 'conffwk.dal.FSMAction'
            self.actions[action.id] = FSMActionFactory.get().get_action(
                action.id, action
            )

        for transition in raw.transitions:  # type: 'conffwk.dal.FSMTransition'
            tr = Transition(
                name=transition.id,
                source=transition.source,
                destination=transition.dest,
                arguments=[],  # list(google.protobuf.any_pb2.Any)
            )

            # Get the pre and post transition sequences for the transition
            pre_transitions: PreOrPostTransitionSequence = (
                self._fill_pre_post_transition_sequence_oks(
                    "pre", tr, raw.pre_transitions
                )
            )
            post_transitions: PreOrPostTransitionSequence = (
                self._fill_pre_post_transition_sequence_oks(
                    "post", tr, raw.post_transitions
                )
            )

            # Add the pre and post transition sequence arguments to the transition
            if tr.arguments is not None:
                tr.arguments += pre_transitions.get_arguments()
                tr.arguments += post_transitions.get_arguments()

            # Store the pre and post transition sequences for the transition
            self._pre_transitions[tr] = pre_transitions
            self._post_transitions[tr] = post_transitions

            # Add the transition to the list of transitions
            self.transitions += [tr]

        # Fill the sequences of commands to execute for each transition
        for sequence in raw.command_sequences:
            seq_id = sequence.id
            cmd_ids = [cmd.id for cmd in sequence.sequence]
            self.sequences.append(FSMSequence(id=seq_id, command_ids=cmd_ids))

    def get_actions(self) -> dict[str, FSMActionProtocol]:
        return self.actions

    def get_initial_state(self) -> str:
        return self.data.initial_state

    def get_states(self) -> list[str]:
        return self.data.states

    def get_transitions(self) -> list[Transition]:
        return self.transitions

    def get_pre_transitions_sequences(
        self,
    ) -> dict[Transition, PreOrPostTransitionSequence]:
        return self.pre_transitions

    def get_post_transitions_sequences(
        self,
    ) -> dict[Transition, PreOrPostTransitionSequence]:
        return self.post_transitions

    def get_sequences(self) -> list[FSMSequence]:
        return self.sequences
