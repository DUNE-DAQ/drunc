from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Optional

from drunc.exceptions import DruncCommandException
from drunc.fsm.core import FSM
from drunc.fsm.exceptions import InvalidTransition
from drunc.fsm.utils import decode_fsm_arguments
from drunc.utils.utils import get_logger

if TYPE_CHECKING:
    from kafkaopmon.OpMonPublisher import OpMonPublisher


class Observed:
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        # if self._broadcast_on_change is None or self._broadcast_key is None:
        #     self._value = value
        #     return

        # self._broadcast_on_change.broadcast(
        #     message = f'Changing {self._name} from {self._value} to {value}',
        #     btype = self._broadcast_key,
        # )
        self._value = value
        if self.stateful_node:
            self.stateful_node.log.info(f"{self._name} changed to {value}")
            self.stateful_node.publish_state()

    def __init__(
        self, name: str, stateful_node=None, initial_value: Optional[str] = None
    ):
        self._name = name
        self.stateful_node = stateful_node
        self._value = initial_value


class OperationalState(Observed):
    def __init__(self, **kwargs):
        super(OperationalState, self).__init__(name="operational_state", **kwargs)


class ErrorState(Observed):
    def __init__(self, **kwargs):
        super(ErrorState, self).__init__(name="error_state", **kwargs)


class InclusionState(Observed):
    def __init__(self, **kwargs):
        super(InclusionState, self).__init__(name="inclusion_state", **kwargs)


class StatefulNodeException(DruncCommandException):
    pass


class CannotInclude(StatefulNodeException):
    def __init__(self):
        super().__init__("Cannot include node (most likely, it is already included)")


class CannotExclude(StatefulNodeException):
    def __init__(self):
        super().__init__("Cannot exclude node (most likely, it is already excluded)")


class InvalidSubTransition(StatefulNodeException):
    def __init__(self, current_state, expected_state, action):
        message = f'SubTransition "{action}" cannot be executed, state needs to be "{expected_state}", it is now "{current_state}"'
        super(InvalidSubTransition, self).__init__(message)


class TransitionNotTerminated(StatefulNodeException):
    def __init__(self):
        super().__init__("The transition did not finished successfully")


class TransitionExecuting(StatefulNodeException):
    def __init__(self):
        super().__init__("A transition is already executing")


class StatefulNode(abc.ABC):
    def __init__(
        self,
        fsm_configuration,
        publisher: Optional[OpMonPublisher] = None,
        session: str = "",
        name: str = "",
    ):
        self.publisher = publisher
        self.session = session
        self.name = name

        self.__fsm = FSM(fsm_configuration)
        self.log = get_logger("controller.StatefulNode")
        self.__operational_state = OperationalState(
            stateful_node=self, initial_value=self.__fsm.initial_state
        )
        self.__operational_sub_state = OperationalState(
            stateful_node=self, initial_value=self.__fsm.initial_state
        )
        self.__included = InclusionState(stateful_node=self, initial_value=True)
        self.__in_error = ErrorState(stateful_node=self, initial_value=False)

    def publish_state(self):
        from druncschema.opmon_pb2 import Status

        if self.publisher is not None:
            self.publisher.publish(
                session=self.session,
                application=self.name,
                message=Status(
                    state=self.__operational_state.value,
                    sub_state=self.__operational_sub_state.value,
                    in_error=self.__in_error.value,
                    included=self.__included.value,
                ),
            )

    def get_node_operational_state(self):
        return self.__operational_state.value

    def get_node_operational_sub_state(self):
        return self.__operational_sub_state.value

    def force_set_node_operational_state(self, state):
        self.__operational_state.value = state

    def force_set_node_operational_sub_state(self, state):
        self.__operational_sub_state.value = state

    def get_fsm_transitions(self):
        r = self.__fsm.get_executable_transitions(self.get_node_operational_state())
        return r

    def get_all_fsm_transitions(self):
        r = self.__fsm.get_all_transitions()
        return r

    def get_fsm_transition(self, transition_name):
        return self.__fsm.get_transition(transition_name)

    def include_node(self):
        if self.__included.value:
            raise CannotInclude()
        self.__included.value = True

    def exclude_node(self):
        if not self.__included.value:
            raise CannotExclude()
        self.__included.value = False

    def node_is_included(self):
        return self.__included.value

    def to_error(self):
        self.__in_error.value = True

    def resolve_error(self):
        self.__in_error.value = False

    def node_is_in_error(self):
        return self.__in_error.value

    def can_transition(self, transition):
        self.log.debug(
            f"{self.__operational_state.value} == {self.__operational_sub_state.value} ?"
        )
        if self.__operational_state.value != self.__operational_sub_state.value:
            return False
        return self.__fsm.can_execute_transition(
            self.get_node_operational_state(), transition
        )

    def decode_fsm_arguments(self, fsm_command):
        transition = self.get_fsm_transition(fsm_command.command_name)
        return decode_fsm_arguments(fsm_command.arguments, transition.arguments)

    def prepare_transition(
        self, transition, transition_data, transition_args, ctx=None
    ):
        if self.get_node_operational_state() != self.get_node_operational_sub_state():
            raise InvalidSubTransition(
                self.get_node_sub_operational_state(),
                self.get_node_operational_state(),
                "prepare_transition",
            )

        if not self.__fsm.can_execute_transition(
            self.get_node_operational_state(), transition
        ):
            raise InvalidTransition(transition, self.get_node_operational_state())

        self.__operational_sub_state.value = f"preparing-{transition.name}"

        transition_data = self.__fsm.prepare_transition(
            transition,
            transition_data,
            transition_args,
            ctx,
        )

        self.__operational_sub_state.value = f"{transition.name}-ready"

        return transition_data

    def propagate_transition_mark(self, transition):
        if self.get_node_operational_sub_state() != f"{transition.name}-ready":
            raise InvalidSubTransition(
                self.get_node_operational_sub_state(),
                f"{transition.name}-ready",
                "propagate_transition",
            )

        self.__operational_sub_state.value = f"propagating-{transition.name}"

    def finish_propagating_transition_mark(self, transition):
        if self.get_node_operational_sub_state() != f"propagating-{transition.name}":
            raise InvalidSubTransition(
                self.get_node_operational_sub_state(),
                f"propagating-{transition.name}",
                "finish_propagating_transition",
            )

        self.__operational_sub_state.value = f"{transition.name}-propagated"

    def start_transition_mark(self, transition):
        if self.get_node_operational_sub_state() != f"{transition.name}-propagated":
            raise InvalidSubTransition(
                self.get_node_operational_sub_state(),
                f"{transition.name}-propagated",
                "start_transition",
            )

        self.__operational_sub_state.value = f"executing-{transition.name}"

    def terminate_transition_mark(self, transition):
        if self.get_node_operational_sub_state() != f"executing-{transition.name}":
            raise InvalidSubTransition(
                self.get_node_operational_sub_state(),
                f"executing-{transition.name}",
                "terminate_transition",
            )

        self.__operational_sub_state.value = f"{transition.name}-terminated"
        self.__operational_state.value = self.__fsm.get_destination_state(
            self.__operational_state.value, transition
        )

    def finalise_transition(
        self, transition, transition_data, transition_args, ctx=None
    ):
        if self.get_node_operational_sub_state() != f"{transition.name}-terminated":
            raise InvalidSubTransition(
                self.get_node_operational_sub_state(),
                f"{transition.name}-terminated",
                "finalise_transition",
            )

        self.__operational_sub_state.value = f"finalising-{transition.name}"
        transition_data = self.__fsm.finalise_transition(
            transition,
            transition_data,
            transition_args,
            ctx,
        )
        self.__operational_sub_state.value = self.__operational_state.value

        return transition_data
