import pytest

import drunc.fsm.exceptions as fsme
from drunc.fsm.core import FSMDestinationType

# ---------------------------------------------------------------------------
# Initialisation tests
# ---------------------------------------------------------------------------


def test_initial_state_is_set(fsm):
    """FSM must store the initial state returned by the configuration."""
    assert fsm.initial_state == "initial"


def test_all_states_loaded(fsm, states):
    """FSM must load all states provided by the configuration."""
    assert set(fsm.states) == set(states)


def test_transition_count(fsm, transitions):
    """FSM must load the exact number of transitions from the configuration."""
    assert len(fsm.transitions) == len(transitions)


# ---------------------------------------------------------------------------
# get_transition tests
# ---------------------------------------------------------------------------


def test_get_transition_returns_correct_object(fsm):
    """Requesting a known transition name must return the matching Transition."""
    tr = fsm.get_transition("conf")
    assert tr.name == "conf"
    assert tr.source == "initial"
    assert tr.destination == "configured"


def test_get_transition_unknown_name_raises(fsm):
    """Requesting an unknown transition name must raise NoTransitionOfName."""
    with pytest.raises(fsme.NoTransitionOfName):
        fsm.get_transition("nonexistent_command")


# ---------------------------------------------------------------------------
# can_execute_transition tests
# ---------------------------------------------------------------------------


def test_can_execute_exact_match(fsm):
    """A transition whose source exactly matches the current state must be executable."""
    tr = fsm.get_transition("conf")  # source = 'initial'
    assert fsm.can_execute_transition("initial", tr) is True


def test_cannot_execute_non_matching_state(fsm):
    """A transition whose source does not match the current state must not be executable."""
    tr = fsm.get_transition("conf")  # source = 'initial'
    assert fsm.can_execute_transition("configured", tr) is False


def test_can_execute_regex_alternation(fsm):
    """change_rate has source 'ready|running'; it must match either state."""
    tr = fsm.get_transition("change_rate")
    assert fsm.can_execute_transition("ready", tr) is True
    assert fsm.can_execute_transition("running", tr) is True


def test_cannot_execute_outside_regex_alternation(fsm):
    """change_rate must not match states outside its source pattern."""
    tr = fsm.get_transition("change_rate")
    assert fsm.can_execute_transition("initial", tr) is False


# ---------------------------------------------------------------------------
# get_destination_state tests
# ---------------------------------------------------------------------------


def test_destination_state_normal_transition(fsm):
    """conf from 'initial' must resolve to 'configured' with a VALID destination type."""
    tr = fsm.get_transition("conf")
    result = fsm.get_destination_state("initial", tr)
    assert result.destination_state == "configured"
    assert result.destination_type == FSMDestinationType.VALID


def test_destination_state_self_loop_empty_destination(fsm):
    """change_rate has an empty destination; source state must be returned with DESTINATION_IS_SOURCE type."""
    tr = fsm.get_transition("change_rate")
    result = fsm.get_destination_state("running", tr)
    assert result.destination_state == "running"
    assert result.destination_type == FSMDestinationType.DESTINATION_IS_SOURCE


def test_destination_state_self_loop_with_destination(fsm):
    """A transition whose destination explicitly matches the source state returns DESTINATION_IS_SOURCE.
    but the source state does not match the required initial state for the transition"""
    tr = fsm.get_transition("conf")
    result = fsm.get_destination_state("configured", tr)
    assert not fsm.can_execute_transition("configured", tr)
    assert result.destination_state == "configured"
    assert result.destination_type == FSMDestinationType.DESTINATION_IS_SOURCE


def test_destination_state_incompatible_source(fsm):
    """Resolving a destination from an incompatible source state must return TRANSITION_NOT_VALID."""
    tr = fsm.get_transition("conf")  # only valid from 'initial'
    result = fsm.get_destination_state("not_initial_state", tr)
    assert result.destination_state is None
    assert result.destination_type == FSMDestinationType.TRANSITION_NOT_VALID


# ---------------------------------------------------------------------------
# get_executable_transitions tests
# ---------------------------------------------------------------------------


def test_only_conf_executable_from_initial(fsm):
    """From 'initial' only the 'conf' transition must be executable."""
    result = fsm.get_executable_transitions("initial")
    assert [t.name for t in result] == ["conf"]


def test_multiple_transitions_executable_from_ready(fsm):
    """From 'ready', enable_triggers, drain_dataflow, and change_rate must all be executable."""
    result = fsm.get_executable_transitions("ready")
    assert {t.name for t in result} == {
        "enable_triggers",
        "drain_dataflow",
        "change_rate",
    }


# ---------------------------------------------------------------------------
# get_executable_sequences tests
# ---------------------------------------------------------------------------


def test_start_run_executable_from_initial(fsm):
    """start_run begins with 'conf' which is valid from 'initial', so it must be returned."""
    result = fsm.get_executable_sequences("initial")
    assert "start_run" in [s.id for s in result]


def test_shutdown_not_executable_from_initial(fsm):
    """shutdown begins with 'disable_triggers' which is invalid from 'initial', so it must not be returned."""
    result = fsm.get_executable_sequences("initial")
    assert "shutdown" not in [s.id for s in result]


def test_stop_run_executable_from_running(fsm):
    """stop_run begins with 'disable_triggers' which is valid from 'running', so it must be returned."""
    result = fsm.get_executable_sequences("running")
    assert "stop_run" in [s.id for s in result]
