# https://github.com/DUNE-DAQ/drunc/issues/363

from drunc.controller.configuration import ControllerConfHandler
from drunc.utils.configuration import OKSKey
from drunc.utils.utils import get_root_logger


def test_issue363(load_test_config):
    get_root_logger("INFO")
    conf_path = "oksconflibs:nestedConfig.data.xml"
    controller_id = "nested-segment-controller"

    controller_configuration = ControllerConfHandler.from_oks(
        url=conf_path,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=controller_id,
            session="test-nested-config",  # some of the function for enable/disable require the full dal of the session
        ),
        session_name="test",
    )
    ids = [segment.id for segment in controller_configuration.segments]
    assert ids == ["bottom-segment-1", "bottom-segment-2"]
    assert controller_configuration.controller.id == controller_id
