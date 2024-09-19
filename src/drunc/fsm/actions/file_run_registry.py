from drunc.fsm.core import FSMAction
from drunc.utils.configuration import find_configuration
from daqconf.consolidate import consolidate_db

class FileRunRegistry(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "file-run-registry"
        )
        self.configuration = configuration

    def pre_start(self, _input_data, _context, **kwargs):
        run_number = _input_data['run']
        run_configuration = find_configuration(_context.configuration.initial_data)

        import shutil
        import os
        
        dest = os.getcwd()+"/run_conf"+str(run_number)+".data.xml"
        consolidate_db(run_configuration, f"{dest}")

        return _input_data
