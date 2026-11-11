from os import environ
from typing import Optional
from pathlib import Path



import conffwk
from sh import Command, ErrorReturnCode  # type: ignore[import-untyped]

from drunc.exceptions import DruncSetupException
from drunc.fsm._protocols import ContextProtocol, InitConfigurationProtocol
from drunc.fsm.core import FSMAction
from drunc.fsm.exceptions import ThreadPinningFailed
from drunc.process_manager.oks_parser import collect_apps
from drunc.process_manager.utils import get_rte_script
from drunc.utils.utils import get_logger


class ReloadConf(FSMAction):
    def __init__(self, configuration: InitConfigurationProtocol) -> None:
        super().__init__(name="reload-configuration")
        self.log = get_logger("controller.iface.reload-configuration")
        self.conf_dict = {p.name: p.value for p in configuration.parameters}

    def reload_conf(self, 
                    new_file: str, 
                    session: str,
                    _input_data: dict[str, object]
    ) -> dict[str, object]:

        # check if the file exists
        file_path = Path(new_file)
        if not file_path.exists():
            raise DruncSetupException(f"Configuration file {new_file} not found")

        # tries to open the file to see if the configuration is valid
        db = conffwk.Configuration('oksconflibs:' + new_file)
        test_dal = db.get_dal(class_name="Session", uid=session)

        # Add the information in the data sent to Applications
        _input_data["reload_configuration"] = True
        _input_data["confspec"] = new_file

        return _input_data

    def pre_reconf(
            self,
            _input_data: dict[str, object],
            _context: ContextProtocol, 
            new_file: Optional[str] = None,
            **kwargs: object,
    ) -> dict[str, object]:

        if not new_file:
            # in this case the file is the same
            new_file = _context.configuration.initial_data

        return self.reload_conf(
            new_file, 
            session=_context.configuration.oks_key.session,
            _input_data=_input_data
        )

