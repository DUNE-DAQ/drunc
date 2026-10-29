import drunc.fsm.configuration as config
import drunc.utils.configuration as utils

from collections import Counter

import pytest


conf_path = "config/drunc/one-controller-config.data.xml"

def test_test_fms() :

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


def test_prod_fms() :

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

    oks_transitions = [t.name for t in configuration.get_transitions()]
    expected_transitions = ["conf", "start", "enable_triggers", "disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "scrap", "change_rate"]
    assert Counter(oks_transitions) == Counter(expected_transitions)

    oks_sequences = [s.id for s in configuration.get_sequences()]
    expected_sequences = ["shutdown", "start_run", "stop_run"]
    assert Counter(oks_sequences) == Counter(expected_sequences)

