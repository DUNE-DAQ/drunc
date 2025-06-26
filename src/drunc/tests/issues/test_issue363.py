# https://github.com/DUNE-DAQ/drunc/issues/363

from drunc.controller.configuration import ControllerConfHandler
from drunc.utils.configuration import OKSKey, parse_conf_url
from drunc.utils.utils import setup_root_logger


def test_issue363(load_test_config):
    setup_root_logger("INFO")
    conf_path, conf_type = parse_conf_url("oksconflibs:nestedConfig.data.xml")
    controller_id = "nested-segment-controller"

    controller_configuration = ControllerConfHandler(
        type=conf_type,
        data=conf_path,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=controller_id,
            session="test-config",  # some of the function for enable/disable require the full dal of the session
        ),
        session_name="test",
    )
    ids = [segment.id for segment in controller_configuration.data.segments]
    assert ids == ["bottom-segment-1", "bottom-segment-2"]
    assert controller_configuration.data.controller.id == controller_id
