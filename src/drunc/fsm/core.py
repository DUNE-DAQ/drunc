# Define the abcs first to avoid circular imports
class FSMAction:
    """Abstract class defining a generic action"""

    def __init__(self, name):
        self.name = name


class Callback:
    def __init__(self, method, mandatory=True):
        self.method = method
        self.mandatory = mandatory


import json
import traceback
from inspect import Parameter, signature
from typing import List

from druncschema.controller_pb2 import Argument
from druncschema.generic_pb2 import bool_msg, float_msg, int_msg, string_msg

import drunc.fsm.exceptions as fsme
from drunc.exceptions import DruncException, DruncSetupException
from drunc.fsm.transition import Transition
from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import get_logger, regex_match


class PreOrPostTransitionSequence:
    def __init__(self, transition: Transition, pre_or_post="pre"):
        self.transition = transition
        if pre_or_post not in ["pre", "post"]:
            raise DruncSetupException(
                f"pre_or_post should be either 'pre' of 'post', you provided '{pre_or_post}'"
            )

        self.prefix = pre_or_post

        self.sequence = []
        self.log = get_logger("controller.PreOrPostTransitionSequence")

    def add_callback(self, action, mandatory=True):
        method = getattr(action, f"{self.prefix}_{self.transition.name}")

        if not method:
            raise DruncSetupException(
                f"{self.prefix}_{self.transition.name} method not found in {action.name}"
            )

        self.sequence += [
            Callback(
                method=method,
                mandatory=mandatory,
            )
        ]

    def __str__(self):
        return ", ".join(
            [f"{cb.method.__name__} (mandatory={cb.mandatory})" for cb in self.sequence]
        )

    def execute(self, transition_data, transition_args, ctx=None):
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
                ctx.runinfo = input_data
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

    def get_arguments(self):
        """Creates a list of arguments
        This is a bit sloppy, as really, I shouldn't be using protobuf here, and convert them later, but...
        """
        retr = []
        all_the_parameter_names = []

        for callback in self.sequence:
            method = callback.method
            s = signature(method)

            for pname, p in s.parameters.items():
                if pname in ["_input_data", "_context", "args", "kwargs"]:
                    continue

                if pname in all_the_parameter_names:
                    raise fsme.DoubleArgument(
                        f"Parameter {pname} is already in the list of parameters"
                    )
                all_the_parameter_names.append(p)

                default_value = ""

                t = Argument.Type.INT

                if p.annotation is str:
                    t = Argument.Type.STRING

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(string_msg(value=p.default))

                elif p.annotation is float:
                    t = Argument.Type.FLOAT

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(float_msg(value=p.default))

                elif p.annotation is int:
                    t = Argument.Type.INT

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(int_msg(value=p.default))

                elif p.annotation is bool:
                    t = Argument.Type.BOOL

                    if p.default != Parameter.empty:
                        default_value = pack_to_any(bool_msg(value=p.default))
                else:
                    raise fsme.UnhandledArgumentType(p.annotation)

                a = Argument(
                    name=p.name,
                    presence=Argument.Presence.MANDATORY
                    if p.default == Parameter.empty
                    else Argument.Presence.OPTIONAL,
                    type=t,
                    help="",
                )

                if default_value:
                    a.default_value.CopyFrom(default_value)

                retr += [a]

        return retr


class FSM:
    def __init__(self, conf):
        self.log = get_logger("controller.FSM")
        self.configuration = conf

        self.initial_state = self.configuration.get_initial_state()
        self.states = self.configuration.get_states()

        self.transitions = self.configuration.get_transitions()
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

    def _enusure_unique_transition(self, transitions):
        a_set = set()
        for t in transitions:
            if t.name in a_set:
                raise fsme.DuplicateTransition(t.name)
            a_set.add(t.name)

    def get_all_states(self) -> List[str]:
        """Grabs all the states"""
        return self.states

    def get_all_transitions(self) -> List[Transition]:
        """Grab all the transitions"""
        return self.transitions

    def get_destination_state(self, source_state, transition) -> str:
        """Tells us where a particular transition will take us, given the source_state"""
        right_name = [t for t in self.transitions if t == transition]
        for tr in right_name:
            if self.can_execute_transition(source_state, transition):
                if tr.destination == "":
                    return source_state
                else:
                    return tr.destination

    def get_executable_transitions(self, source_state) -> List[Transition]:
        valid_transitions = []

        for tr in self.transitions:
            debug_txt = f'Testing if transition {tr!s} is executable from state "{source_state}"...'
            if self.can_execute_transition(source_state, tr):
                self.log.debug(f"{debug_txt} Yes")
                valid_transitions.append(tr)
            else:
                self.log.debug(f"{debug_txt} No\n")

        return valid_transitions

    def get_transition(self, transition_name) -> bool:
        self.log.debug(f"Searching for transition {transition_name}")
        transition = [t for t in self.transitions if t.name == transition_name]
        self.log.debug(f"Found transition {transition}")
        if not transition:
            raise fsme.NoTransitionOfName(transition_name)
        return transition[0]

    def can_execute_transition(self, source_state, transition) -> bool:
        """Check that this transition is allowed given the source_state"""
        self.log.debug(f"can_execute_transition {transition.source!s} {source_state}")
        return regex_match(transition.source, source_state)

    def prepare_transition(
        self, transition, transition_data, transition_args, ctx=None
    ):
        transition_data = self.pre_transition_sequences[transition].execute(
            transition_data, transition_args, ctx
        )
        return transition_data

    def finalise_transition(
        self, transition, transition_data, transition_args, ctx=None
    ):
        transition_data = self.post_transition_sequences[transition].execute(
            transition_data, transition_args, ctx
        )
        return transition_data
