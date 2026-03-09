from unittest.mock import MagicMock, patch

import pytest
from druncschema.controller_pb2 import FSMSequence

from drunc.fsm.core import FSM
from drunc.fsm.transition import Transition

# ---------------------------------------------------------------------------
# Constants as fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def states():
    """Full list of FSM states, matching a real controller configuration."""
    return [
        "initial",
        "configured",
        "ready",
        "running",
        "paused",
        "dataflow_drained",
        "trigger_sources_stopped",
        "error",
    ]


@pytest.fixture
def transitions():
    """Full list of FSM transitions, matching a real controller configuration."""
    return [
        Transition(
            name="conf", source="initial", destination="configured", arguments=[]
        ),
        Transition(
            name="start", source="configured", destination="ready", arguments=[]
        ),
        Transition(
            name="enable_triggers", source="ready", destination="running", arguments=[]
        ),
        Transition(
            name="disable_triggers", source="running", destination="ready", arguments=[]
        ),
        Transition(
            name="drain_dataflow",
            source="ready",
            destination="dataflow_drained",
            arguments=[],
        ),
        Transition(
            name="stop_trigger_sources",
            source="dataflow_drained",
            destination="trigger_sources_stopped",
            arguments=[],
        ),
        Transition(
            name="stop",
            source="trigger_sources_stopped",
            destination="configured",
            arguments=[],
        ),
        Transition(
            name="scrap", source="configured", destination="initial", arguments=[]
        ),
        Transition(
            name="change_rate", source="ready|running", destination="", arguments=[]
        ),
    ]


@pytest.fixture
def sequences():
    """Full list of FSM sequences, matching a real controller configuration."""
    return [
        FSMSequence(
            id="shutdown",
            command_ids=[
                "disable_triggers",
                "drain_dataflow",
                "stop_trigger_sources",
                "stop",
                "scrap",
            ],
        ),
        FSMSequence(id="start_run", command_ids=["conf", "start", "enable_triggers"]),
        FSMSequence(
            id="stop_run",
            command_ids=[
                "disable_triggers",
                "drain_dataflow",
                "stop_trigger_sources",
                "stop",
            ],
        ),
    ]


# ---------------------------------------------------------------------------
# Configuration and FSM fixtures
# ---------------------------------------------------------------------------


class _NullSequence:
    """
    Minimal pre/post-transition sequence stub that does nothing and returns
    an empty JSON object string — matching the real PreOrPostTransitionSequence
    which always returns json.dumps(input_data).
    """

    def execute(self, transition_data, transition_args, ctx=None):
        return "{}"

    def __str__(self):
        return ""


@pytest.fixture
def mock_conf(transitions, sequences, states):
    """
    Mocked FSMConfHandler whose methods return the standard set of states,
    transitions, and sequences — mirroring the real return types from
    FSMConfHandler exactly.
    """
    conf = MagicMock()
    conf.get_initial_state.return_value = "initial"
    conf.get_states.return_value = states
    conf.get_transitions.return_value = transitions
    conf.get_sequences.return_value = sequences

    # Keyed by Transition object, matching the real pre_transitions /
    # post_transitions dicts built in FSMConfHandler._post_process_oks
    null_seq = _NullSequence()
    conf.get_pre_transitions_sequences.return_value = {t: null_seq for t in transitions}
    conf.get_post_transitions_sequences.return_value = {
        t: null_seq for t in transitions
    }

    return conf


@pytest.fixture
def fsm(mock_conf):
    """
    Real FSM instance built from the mocked configuration. get_logger is
    patched at its import site inside drunc.fsm.core so that log calls do
    not appear in test output.
    """
    with patch("drunc.fsm.core.get_logger", return_value=MagicMock()):
        return FSM(mock_conf)
