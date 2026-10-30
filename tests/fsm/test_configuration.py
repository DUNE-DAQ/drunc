from grpc import RpcError

from drunc.fsm.exceptions import DotDruncJsonNotFound
import drunc.fsm.configuration as config
import drunc.utils.configuration as utils

from collections import Counter

import pytest

## I should come up with some wrong configurations to test the exceptions

def test_test_fsm() :
    conf_path = "config/drunc/one-controller-config.data.xml"
    configuration = config.FSMConfHandler.from_oks(
        url="oksconflibs:" + conf_path,
        oks_key=utils.OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="FSMconfiguration",
            obj_uid="fsmConf-test",
            session="one-controller-config", 
        ),
        session_name="test",
    )

    oks_transitions = [t.name for t in configuration.get_transitions()]
    expected_transitions = ["conf", "start", "enable_triggers", "disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "scrap", "change_rate"]
    assert Counter(oks_transitions) == Counter(expected_transitions)

    oks_sequences = [s.id for s in configuration.get_sequences()]
    expected_sequences = ["shutdown", "start_run", "stop_run"]
    assert Counter(oks_sequences) == Counter(expected_sequences)

    oks_actions = [a for a in configuration.get_actions().keys()]
    expected_actions = ['user-provided-run-number', 'file-run-registry', 'trigger-rate-specifier']
    assert Counter(oks_actions) == Counter(expected_actions)
    

def test_prod_fsm() :
    # the actions of prod FSM require a drunc.json file which is not expected
    # so this test is supposed to fail
    conf_path = "config/drunc/one-controller-config.data.xml"

    with pytest.raises(DotDruncJsonNotFound):
        configuration = config.FSMConfHandler.from_oks(
            url="oksconflibs:" + conf_path,
            oks_key=utils.OKSKey(
                schema_file="schema/confmodel/dunedaq.schema.xml",
                class_name="FSMconfiguration",
                obj_uid="fsmConf-prod",
                session="one-controller-config", 
            ),
            session_name="test",
        )

def test_reduntant_commands_fsm() :
    # the actions of prod FSM require a drunc.json file which is not expected
    # so this test is supposed to fail
    conf_path = "config/drunc/fsm_with_errors.data.xml"

    with pytest.raises(ValueError):
        configuration = config.FSMConfHandler.from_oks(
            url="oksconflibs:" + conf_path,
            oks_key=utils.OKSKey(
                schema_file="schema/confmodel/dunedaq.schema.xml",
                class_name="FSMconfiguration",
                obj_uid="redundant_transitions",
                session="one-controller-config", 
            ),
            session_name="test",
        )

    with pytest.raises(ValueError):
        configuration = config.FSMConfHandler.from_oks(
            url="oksconflibs:" + conf_path,
            oks_key=utils.OKSKey(
                schema_file="schema/confmodel/dunedaq.schema.xml",
                class_name="FSMconfiguration",
                obj_uid="conflicting_sequence",
                session="one-controller-config", 
            ),
            session_name="test",
        )

    with pytest.raises(ValueError):
        configuration = config.FSMConfHandler.from_oks(
            url="oksconflibs:" + conf_path,
            oks_key=utils.OKSKey(
                schema_file="schema/confmodel/dunedaq.schema.xml",
                class_name="FSMconfiguration",
                obj_uid="unrelated_transition",
                session="one-controller-config", 
            ),
            session_name="test",
        )


