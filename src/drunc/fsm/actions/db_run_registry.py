from drunc.fsm.core import FSMAction
from drunc.utils.configuration import find_configuration
from oksconfgen.consolidate import consolidate_db
import json

class DBRunRegistry(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "db-run-registry"
        )        
        f = open(".drunc.json")
        dotdrunc = json.load(f)
        self.API_SOCKET = dotdrunc["run_registry_configuration"]["socket"]
        self.API_USER = dotdrunc["run_registry_configuration"]["user"]
        self.API_PSWD = dotdrunc["run_registry_configuration"]["password"]

        print(configuration)

        import logging
        self._log = logging.getLogger('microservice-run-registry')

    def pre_start(self, _input_data, _context, **kwargs):
        run_number = _input_data['run']
        run_configuration = find_configuration(_context.configuration.initial_data) #
        run_type = _input_data.get("run_type", "TEST")

        with tempfile.NamedTemporaryFile(suffix='.data.xml', delete=False) as f:
            fname = f.name
            consolidate_db(fname, run_configuration)

            with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as f:
                with tarfile.open(fileobj=f, mode='w:gz') as tar:
                    tar.add(fname, arcname=os.path.basename(fname))
                f.flush()
                f.seek(0)
                fname = f.name
    
        import shutil
        import os

        dest = os.getcwd()+"/run_conf"+str(run_number)+".data.xml"
        shutil.copyfile(f"{fname}.tar.gz", dest)
        # insertRun
        

    def post_drain_dataflow(self, _input_data, _context, **kwargs):
        run_number = _input_data['run']
        run_configuration = find_configuration(_context.configuration.initial_data)
        run_type = _input_data.get("run_type", "TEST")

        import shutil
        import os

        # updateStopTime