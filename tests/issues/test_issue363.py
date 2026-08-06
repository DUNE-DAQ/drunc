# https://github.com/DUNE-DAQ/drunc/issues/363

import os

from drunc.controller.configuration import ControllerConfHandler
from drunc.utils.configuration import OKSKey
from drunc.utils.utils import get_root_logger


def test_issue363(load_test_config):
    get_root_logger("INFO")
    conf_path = "config/drunc/nestedConfig.data.xml"

    path_found: bool = False
    for path in os.getenv("DUNEDAQ_DB_PATH", "").split(":"):
        if os.path.exists(os.path.join(path, conf_path)):
            print(f"Found nestedConfig.data.xml in {path}")
            path_found = True
            break

    if not path_found:
        raise FileNotFoundError(
            "nestedConfig.data.xml not found in any of the paths specified in DUNEDAQ_DB_PATH"
        )

    controller_id = "nested-segment-controller"

    controller_configuration = ControllerConfHandler.from_oks(
        url="oksconflibs:" + conf_path,
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
