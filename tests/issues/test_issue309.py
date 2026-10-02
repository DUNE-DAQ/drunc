# https://github.com/DUNE-DAQ/drunc/issues/309

from drunc.controller.configuration import ControllerConfHandler
from drunc.utils.utils import get_root_logger


def test_issue309(load_test_config):
    get_root_logger("INFO")
    from drunc.utils.configuration import OKSKey

    conf_path = "oksconflibs:deep-segments-config.data.xml"
    controller_id = "controller-3"

    controller_configuration = ControllerConfHandler.from_oks(
        url=conf_path,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=controller_id,
            session="deep-segments-config",  # some of the function for include/exclude requires the full dal of the session
        ),
        session_name="test",
    )

    assert controller_configuration.controller.id == controller_id
