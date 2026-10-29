from __future__ import annotations

from curses import raw
from typing import List, cast

from conffwk.dal import FSMData, FSMxTransition
from druncschema.controller_pb2 import FSMSequence

from drunc.fsm._protocols import FSMActionProtocol
from drunc.fsm.action_factory import FSMActionFactory
from drunc.fsm.core import PreOrPostTransitionSequence
from drunc.fsm.transition import Transition
from drunc.utils.configuration import ConfHandler
from drunc.utils.utils import get_logger


class FSMConfHandler(ConfHandler):
    """Handler for FSM configuration."""

    data: FSMData

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
            ValuesError: If there are duplicate transition or sequence names, or if a sequence references a non-existent transition. 
        """
        raw = cast(FSMData, self._raw_data)

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

        ## fill the transistions 
        for transition in raw.transitions:

            ## check if the transition name is unique
            if transition.cmd in [t.name for t in self.transitions]:
                raise ValueError(
                    f"Duplicate transition name '{transition.cmd}' from transition object '{transition.id}'"
                )

            tr = Transition(
                name=transition.cmd,
                source=transition.source,
                destination=transition.dest,
                arguments=[],  # list(google.protobuf.any_pb2.Any)
            )

            arguments = []
            pre = transition.pre_transition
            if pre is not None:
                seq = PreOrPostTransitionSequence(tr,"pre")
                ## add the action
                for a in pre.actions :
                    ## The action could be registerered by many transisionts 
                    if a.name not in self.actions:
                        self.actions[a.name] = FSMActionFactory.get().get_action(
                            a.name, a 
                    )
                    seq.add_callback(
                        action=self.actions[a.name],
                        mandatory=a.name in pre.mandatory,
                    )    
                arguments += seq.get_arguments()
                self.pre_transitions[tr] = seq
                            
            post = transition.post_transition
            if post is not None:
                seq = PreOrPostTransitionSequence(tr,"post")
                ## add the action
                for a in post.actions :
                    ## The action could be registerered by many transisionts 
                    if a.name not in self.actions:
                        self.actions[a.name] = FSMActionFactory.get().get_action(
                            a.name, a 
                    )
                    seq.add_callback(
                        action=self.actions[a.name],
                        mandatory=a.name in post.mandatory,
                    )    
                arguments += seq.get_arguments()
                self.post_transitions[tr] = seq

            ## fill the arguments
            ## at this points all the actions are registered we just need to register arguments
            if arguments is not None:
                tr.arguments += arguments
                
            self.transitions.append(tr)

        ## fill anche check the sequences
        tr_ids = [t.id for t in raw.transitions]

        for sequence in raw.command_sequences:
            seq_id = sequence.name 
            if seq_id in [t.name for t in self.transitions]:
                raise ValueError(
                    f"Duplicate transition name '{seq_id}' from sequence object'{sequence.id}'"
                ) 
            if seq_id in [s.id for s in self.sequences]:
                raise ValueError(
                    f"Duplicate sequence name '{seq_id}' from sequence object'{sequence.id}'"
                ) 

            ## Here we check a bit more
            # we check if the OKS objects used in the sequence are the 
            # same as used in the transitions 
            cmd_names=[]
            for tr in sequence.sequence:
                if tr.id not in tr_ids:
                    raise ValueError(
                        f"Transition id '{tr.id}' from sequence object '{sequence.id}' does not match any transitions in this FSM configuration"
                    )
                cmd_names.append(tr.cmd)
            
            self.sequences.append(FSMSequence(id=seq_id, command_ids=cmd_names))
        

    def get_actions(self) -> dict[str, FSMActionProtocol]:
        return self.actions

    def get_initial_state(self) -> str:
        return self.initial_state

    def get_states(self) -> list[str]:
        return self.states

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
