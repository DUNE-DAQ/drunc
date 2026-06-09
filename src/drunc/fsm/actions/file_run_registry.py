import os
from typing import Dict, Protocol

from daqconf.consolidate import consolidate_db  # type: ignore[import-untyped]

from drunc.fsm._protocols import ContextProtocol
from drunc.fsm.core import FSMAction


class FileRunRegistry(FSMAction):
    def __init__(self, configuration: object) -> None:
        super().__init__(name="file-run-registry")
        self.configuration = configuration

    def pre_start(self, _input_data: Dict[str, object], _context: ContextProtocol, **kwargs: object) -> Dict[str, object]:
        run_number = _input_data["run"]
        dest = os.getcwd() + "/run_conf" + str(run_number) + ".data.xml"
        consolidate_db(_context.configuration.initial_data.split(":")[1], f"{dest}")

        return _input_data
