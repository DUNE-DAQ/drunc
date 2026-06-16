from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional, Union

if TYPE_CHECKING:
    from drunc.fsm._protocols import (
        ActionMethodProtocol,
        ConfigProtocol,
        ContextProtocol,
        FSMActionProtocol,
    )


# Define the abcs first to avoid circular imports
class FSMAction:
    """Abstract class defining a generic action"""

    def __init__(self, name: str) -> None:
        self.name = name


class Callback:
    def __init__(self, method: ActionMethodProtocol, mandatory: bool = True) -> None:
        self.method: ActionMethodProtocol = method
        self.mandatory: bool = mandatory


import json
import traceback
from dataclasses import dataclass
from enum import Enum
from inspect import Parameter, signature

from druncschema.controller_pb2 import Argument, FSMSequence
from druncschema.generic_pb2 import bool_msg, float_msg, int_msg, string_msg
from google.protobuf import any_pb2

import drunc.fsm.exceptions as fsme
from drunc.exceptions import DruncException, DruncSetupException
from drunc.fsm.transition import Transition
from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import get_logger, regex_match


class PreOrPostTransitionSequence:
    def __init__(self, transition: Transition, pre_or_post: str = "pre") -> None:
        self.transition: Transition = transition
        if pre_or_post not in ["pre", "post"]:
            raise DruncSetupException(
                f"pre_or_post should be either 'pre' of 'post', you provided '{pre_or_post}'"
            )

        self.prefix: str = pre_or_post

        self.sequence: list[Callback] = []
        self.log = get_logger("controller.core.PreOrPostTransitionSequence")

    def add_callback(self, action: FSMActionProtocol, mandatory: bool = True) -> None:
        """
        Add a callback to the sequence. The method to be called will be determined by
        the name of the transition and the prefix (pre or post).

        For example, if the transition is "start" and the prefix is "pre", the method to
        be called will be "pre_start".

        Args:
            action (conffwk.dal.FSMAction): The action to be added to the sequence.
            mandatory (bool): Whether the callback is mandatory or not.

        Returns:
            None

        Raises:
            DruncSetupException: If the method to be called is not found in the action.
        """

        # Get the method to be called from the action, based on the name of the
        # transition and the prefix (pre or post)
        method = getattr(action, f"{self.prefix}_{self.transition.name}")

        # Sanity check
        if not method:
            raise DruncSetupException(
                f"{self.prefix}_{self.transition.name} method not found in {action.name}"
            )

        # Add the callback to the sequence
        self.sequence += [
            Callback(
                method=method,
                mandatory=mandatory,
            )
        ]

    def __str__(self) -> str:
        return ", ".join(
            [
                f"{cb.method.__self__.__class__.__name__} (mandatory={cb.mandatory})"
                for cb in self.sequence
            ]
        )

    def execute(
        self,
        transition_data: str | None,
        transition_args: Dict[str, object],
        ctx: ContextProtocol,
    ) -> str:
        self.log.debug(f"{transition_data=}, {transition_args=}")
        if not transition_data:
            transition_data = "{}"

        try:
            input_data = json.loads(transition_data)
        except:
            raise fsme.TransitionDataOfIncorrectFormat(transition_data)

        for callback in self.sequence:
            try:
                self.log.debug(f"data before callback: {input_data}")
                self.log.debug(
                    f"executing the callback: {callback.method.__name__} from {callback.method.__module__}"
                )
                input_data = callback.method(
                    _input_data=input_data, _context=ctx, **transition_args
                )
                self.log.debug(f"data after callback: {input_data}")
                if input_data:
                    ctx.runinfo.update(input_data)

                try:
                    json.dumps(input_data)
                except TypeError:
                    raise fsme.InvalidDataReturnByFSMAction(input_data)

            except DruncException as e:
                self.log.error(traceback.format_exc())
                if callback.mandatory:
                    raise e

        self.log.debug(f"data returned: {input_data}")

        return json.dumps(input_data)

    def get_arguments(self) -> list[Argument]:
        """
        Create a list of arguments.

        This is a bit sloppy, as really, I shouldn't be using protobuf here, and convert them later, but...
        Thanks Pierre :/

        Args:
            None

        Returns:
            list(Argument): A list of arguments that the sequence requires.

        Raises:
            fsme.UnhandledArgumentType: If the type of an argument is not one of the
                following: str, float, int, bool, Optional[str], Optional[float],
                Optional[int], Optional[bool], Union[str, None], Union[float, None],
                Union[int, None], Union[bool, None]
        """

        # Construct the list of arguments by looking at the signature of the methods
        arguments: list[Argument] = []

        # Check that there are no duplicate parameter names across the callbacks
        # otherwise, we won't know which one to use when executing the sequence
        all_sequence_arguments: set[str] = set()  # set(Argument names)

        # Iterate over the callbacks, construct the list of arguments
        for callback in self.sequence:
            # Get the signature of the method to determine the arguments
            method = callback.method
            s = signature(method)

            # Iterate over the parameters of the method
            for pname, p in s.parameters.items():
                # Skip the special parameters that are used to pass the input data and
                # context to the callbacks
                if pname in ["_input_data", "_context", "args", "kwargs"]:
                    continue

                # Check that the parameter name is not already in the list of arguments
                if pname in all_sequence_arguments:
                    raise fsme.DoubleArgument(
                        f"Parameter {pname} is already in the list of parameters"
                    )

                # Keep track of the parameter names to check for duplicates
                all_sequence_arguments.add(pname)

                # Set the default value to an empty string, as protobuf doesn't allow
                # using default None
                default_value: any_pb2.Any | None = None

                # Determine the type of the argument, and set the default value if it is
                # optional. If the type is not one of the supported types, raise an
                # error
                t: int = Argument.Type.INT

                if p.annotation in (str, Optional[str], Union[str, None]):
                    t = Argument.Type.STRING

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(string_msg(value=p.default))

                elif p.annotation in (float, Optional[float], Union[float, None]):
                    t = Argument.Type.FLOAT

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(float_msg(value=p.default))

                elif p.annotation in (int, Optional[int], Union[int, None]):
                    t = Argument.Type.INT

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(int_msg(value=p.default))

                elif p.annotation in (bool, Optional[bool], Union[bool, None]):
                    t = Argument.Type.BOOL

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(bool_msg(value=p.default))

                else:
                    raise fsme.UnhandledArgumentType(p.annotation)

                presence = Argument.Presence.MANDATORY
                if default_value or p.annotation in (
                    Optional[str],
                    Optional[float],
                    Optional[int],
                    Optional[bool],
                    Union[str, None],
                    Union[float, None],
                    Union[int, None],
                    Union[bool, None],
                ):
                    presence = Argument.Presence.OPTIONAL

                a = Argument(
                    name=p.name,
                    presence=presence,
                    type=t,
                    help="",
                )

                if default_value:
                    a.default_value.CopyFrom(default_value)
                arguments += [a]

        return arguments


class FSMDestinationType(Enum):
    # The transition is valid and has a destination that is different from the source
    VALID = ("valid",)
    # The transition is a self-loop, the destination is the same as the source
    DESTINATION_IS_SOURCE = ("destination_is_source",)
    # The transition provided is not valid for the source state
    TRANSITION_NOT_VALID = ("transition_not_valid",)


@dataclass
class FSMDestinationResult:
    destination_state: str | None
    destination_type: FSMDestinationType


class FSM:
    def __init__(self, conf: ConfigProtocol) -> None:
        self.log = get_logger("controller.core.FSM")
        self.configuration = conf

        self.initial_state = self.configuration.get_initial_state()
        self.states = self.configuration.get_states()

        self.transitions: list[Transition] = self.configuration.get_transitions()
        self.sequences = self.configuration.get_sequences()
        self._enusure_unique_transition(self.transitions)
        self.pre_transition_sequences = (
            self.configuration.get_pre_transitions_sequences()
        )
        self.post_transition_sequences = (
            self.configuration.get_post_transitions_sequences()
        )

        self.log.debug(f'Initial state is "{self.initial_state}"')
        self.log.debug("Allowed transitions are:")
        for t in self.transitions:
            self.log.debug(str(t))
            self.log.debug(f"Pre transition: {self.pre_transition_sequences[t]}")
            self.log.debug(f"Post transition: {self.post_transition_sequences[t]}")

    def _enusure_unique_transition(self, transitions: list[Transition]) -> None:
        a_set = set()
        for t in transitions:
            if t.name in a_set:
                raise fsme.DuplicateTransition(t.name)
            a_set.add(t.name)

    def get_all_states(self) -> list[str]:
        """Grabs all the states"""
        return self.states

    def get_all_transitions(self) -> list[Transition]:
        """Grab all the transitions"""
        return self.transitions

    def get_all_sequences(self) -> list[FSMSequence]:
        """Grab all the transitions"""
        return self.sequences

    def is_destination_of_this_transition(
        self, state: str, transition: Transition
    ) -> bool:
        return bool(transition.destination == state)

    def get_destination_state(
        self, source_state: str, transition: Transition
    ) -> FSMDestinationResult:
        """Tells us where a particular transition will take us, given the source_state"""
        right_name = [t for t in self.transitions if t == transition]

        for tr in right_name:
            if self.can_execute_transition(source_state, transition):
                if tr.destination == "":
                    # if no destination is provided by transition, assume it's source -> source
                    return FSMDestinationResult(
                        destination_state=source_state,
                        destination_type=FSMDestinationType.DESTINATION_IS_SOURCE,
                    )
                else:
                    # found a transition that matches the source state provided, return the new destination
                    return FSMDestinationResult(
                        destination_state=tr.destination,
                        destination_type=FSMDestinationType.VALID,
                    )
            else:
                if tr.destination == source_state:
                    # if the transition is not valid from the source state provided,
                    # but its destination is the same as the source state, return that information
                    return FSMDestinationResult(
                        destination_state=source_state,
                        destination_type=FSMDestinationType.DESTINATION_IS_SOURCE,
                    )

        # no transitions match the source state provided
        # or the transition doesn't exist at all
        return FSMDestinationResult(
            destination_state=None,
            destination_type=FSMDestinationType.TRANSITION_NOT_VALID,
        )

    def get_executable_transitions(self, source_state: str) -> list[Transition]:
        valid_transitions = []

        for tr in self.transitions:
            debug_txt = f'Testing if transition {tr!s} is executable from state "{source_state}"...'
            if self.can_execute_transition(source_state, tr):
                self.log.debug(f"{debug_txt} Yes")
                valid_transitions.append(tr)
            else:
                self.log.debug(f"{debug_txt} No\n")

        return valid_transitions

    def get_executable_sequences(self, source_state: str) -> list[FSMSequence]:
        valid_sequences = []

        for seq in self.sequences:
            for cmd_ids in seq.command_ids:
                try:
                    transition = self.get_transition(cmd_ids)
                    if self.can_execute_transition(source_state, transition):
                        valid_sequences.append(seq)
                except fsme.NoTransitionOfName:
                    self.log.debug(
                        f"Skipping sequence {seq.id}, unknown command {cmd_ids}"
                    )
                    continue

        return valid_sequences

    def get_transition(self, transition_name: str) -> Transition:
        self.log.debug(f"Searching for transition {transition_name}")
        transition = [t for t in self.transitions if t.name == transition_name]
        self.log.debug(f"Found transition {transition}")
        if not transition:
            raise fsme.NoTransitionOfName(transition_name)
        return transition[0]

    def can_execute_transition(self, source_state: str, transition: Transition) -> bool:
        """Check that this transition is allowed given the source_state"""
        self.log.debug(f"can_execute_transition {transition.source!s} {source_state}")
        return bool(regex_match(transition.source, source_state))

    def prepare_transition(
        self,
        transition: Transition,
        transition_data: str,
        transition_args: Dict[str, object],
        ctx: ContextProtocol,
    ) -> str:
        transition_data = self.pre_transition_sequences[transition].execute(
            transition_data, transition_args, ctx
        )
        return transition_data

    def finalise_transition(
        self,
        transition: Transition,
        transition_data: str,
        transition_args: Dict[str, object],
        ctx: ContextProtocol,
    ) -> str:
        transition_data = self.post_transition_sequences[transition].execute(
            transition_data, transition_args, ctx
        )
        return transition_data
