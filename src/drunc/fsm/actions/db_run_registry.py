from drunc.fsm.core import FSMAction
from drunc.utils.configuration import find_configuration
from daqconf.consolidate import consolidate_db
import json
import tempfile
import tarfile
import os
import requests

class DBRunRegistry(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "db-run-registry"
        )        
        from drunc.utils.utils import expand_path
        f = open(expand_path("~/.drunc.json")) # cp /nfs/home/titavare/dunedaq_work_area/drunc-n24.5.26-1/.drunc.json        
        dotdrunc = json.load(f)
        self.API_SOCKET = dotdrunc["run_registry_configuration"]["socket"]
        self.API_USER = dotdrunc["run_registry_configuration"]["user"]
        self.API_PSWD = dotdrunc["run_registry_configuration"]["password"]
        self.timeout = 2

        import logging
        self._log = logging.getLogger('microservice-run-registry')

    def pre_start(self, _input_data:dict, _context, **kwargs):
        self.run_number = _input_data['run'] #Seems like run_number isn't in _input_data in post_drain_dataflow so need to initialise it here
        run_configuration = find_configuration(_context.configuration.initial_data) 
        run_type = _input_data.get("run_type", "TEST")
        det_id = _context.configuration.db.get_dal(class_name = "Session", uid = _context.configuration.oks_key.session).detector_configuration.id
        software_version = os.getenv("DUNE_DAQ_BASE_RELEASE")
        from drunc.fsm.exceptions import CannotGetSoftwareVersion
        if software_version == None:
            raise CannotGetSoftwareVersion()
        with tempfile.NamedTemporaryFile(suffix='.data.xml', delete=True) as f:
            f.flush()
            f.seek(0)
            fname = f.name
            consolidate_db(run_configuration, f"{fname}")

            with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tar_f:
                with tarfile.open(fileobj=tar_f, mode='w:gz') as tar:
                    tar.add(fname, arcname=os.path.basename(fname))
                f.flush()
                f.seek(0)
                tar_fname = tar_f.name

            with open(tar_fname, "rb") as f:
                files = {'file': f}
                post_data = {"run_num": self.run_number,
                "det_id": det_id,
                "run_type": run_type,
                "software_version": software_version}
                from drunc.fsm.exceptions import CannotInsertRunNumber
                try:
                    r = requests.post(self.API_SOCKET+"/runregistry/insertRun/",
                                      files=files,
                                      data=post_data,
                                      auth=(self.API_USER, self.API_PSWD),
                                      timeout=self.timeout)
                    r.raise_for_status()
                except requests.HTTPError as exc:
                    error = f"of HTTP Error (maybe failed auth, maybe ill-formed post message, ...) using {__name__}"
                    self._log.error(error)
                    raise CannotInsertRunNumber(error) from exc
                except requests.ConnectionError as exc:
                    error = f"connection to {self.API_SOCKET} wasn't successful using {__name__}"
                    self._log.error(error)
                    raise CannotInsertRunNumber(error) from exc
                except requests.Timeout as exc:
                    error = f"connection to {self.API_SOCKET} timed out using {__name__}"
                    self._log.error(error)
                    raise CannotInsertRunNumber(error) from exc
            os.remove(tar_fname)
        return _input_data
        

    def post_drain_dataflow(self, _input_data, _context, **kwargs):
        from drunc.fsm.exceptions import CannotUpdateStopTime
        try:
            r = requests.get(self.API_SOCKET+"/runregistry/updateStopTime/"+str(self.run_number), 
            auth=(self.API_USER, self.API_PSWD),
            timeout=self.timeout)

        except requests.HTTPError as exc:
            error = f"of HTTP Error (maybe failed auth, maybe ill-formed post message, ...) using {__name__}"
            self._log.error(error)
            raise CannotUpdateStopTime(error) from exc
        except requests.ConnectionError as exc:
            error = f"connection to {self.API_SOCKET} wasn't successful using {__name__}"
            self._log.error(error)
            raise CannotUpdateStopTime(error) from exc
        except requests.Timeout as exc:
            error = f"connection to {self.API_SOCKET} timed out using {__name__}"
            self._log.error(error)
            raise CannotUpdateStopTime(error) from exc