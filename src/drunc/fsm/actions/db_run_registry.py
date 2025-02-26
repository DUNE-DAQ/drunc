import os
import tarfile
import tempfile

import requests
from daqconf.consolidate import consolidate_db
from daqconf.jsonify import jsonify_xml_data

from drunc.fsm.actions.utils import get_dotdrunc_json
from drunc.fsm.core import FSMAction
from drunc.fsm.exceptions import (
    CannotGetSoftwareVersion,
    CannotInsertRunNumber,
    CannotUpdateStopTime,
    DotDruncJsonIncorrectFormat,
)
from drunc.utils.configuration import find_configuration
from drunc.utils.utils import get_logger


class DBRunRegistry(FSMAction):
    def __init__(self, configuration):
        super().__init__(name="db-run-registry")
        self.log = get_logger("controller.usvc_db_run_registry")

        dotdrunc = get_dotdrunc_json()
        try:
            rrc = dotdrunc["run_registry_configuration"]
            self.API_SOCKET = rrc["socket"]
            self.API_USER = rrc["user"]
            self.API_PSWD = rrc["password"]
        except KeyError as exc:
            raise DotDruncJsonIncorrectFormat(
                "Malformed ~/.drunc.json, missing a key in the 'run_registry_configuration' section, or the entire 'run_registry_configuration' section"
            ) from exc
        self.timeout = 2

    def pre_start(self, _input_data: dict, _context, **kwargs):
        self.run_number = _input_data[
            "run"
        ]  # Seems like run_number isn't in _input_data in post_drain_dataflow so need to initialise it here
        run_configuration = find_configuration(_context.configuration.initial_data)
        run_type = _input_data.get("production_vs_test", "TEST")
        det_id = _context.configuration.db.get_dal(
            class_name="Session", uid=_context.configuration.oks_key.session
        ).detector_configuration.id
        software_version = os.getenv("DUNE_DAQ_BASE_RELEASE")
        if software_version == None:
            raise CannotGetSoftwareVersion()

        f_xml = tempfile.NamedTemporaryFile(suffix=".data.xml", delete=True)
        f_json = tempfile.NamedTemporaryFile(suffix=".data.json", delete=True)
        f_entry_point = tempfile.NamedTemporaryFile(
            suffix="_entry_point.txt", delete=True
        )
        xml_name = f_xml.name
        json_name = f_json.name
        entry_point_name = f_entry_point.name

        consolidate_db(run_configuration, xml_name)
        jsonify_xml_data(xml_name, json_name)
        with open(entry_point_name, "w") as f:
            f.write(_context.configuration.oks_key.session)

        f_tar = tempfile.NamedTemporaryFile(
            suffix=".tar.gz",
            delete=False,  # delete when f_tar gets out of scope
            # for after python 3.12...
            # delete = True, # delete when f_tar gets out of scope
            # delete_on_close = False
        )
        tar_name = f_tar.name

        with tarfile.open(fileobj=f_tar, mode="w:gz") as tar:
            tar.add(xml_name, arcname=os.path.basename(xml_name))
            tar.add(json_name, arcname=os.path.basename(json_name))
            tar.add(entry_point_name, arcname=os.path.basename(entry_point_name))
        f_tar.close()

        with open(tar_name, "rb") as f:
            files = {"file": f}
            post_data = {
                "run_num": self.run_number,
                "det_id": det_id,
                "run_type": run_type,
                "software_version": software_version,
            }

            try:
                r = requests.post(
                    self.API_SOCKET + "/runregistry/insertRun/",
                    files=files,
                    data=post_data,
                    auth=(self.API_USER, self.API_PSWD),
                    timeout=self.timeout,
                )
                r.raise_for_status()
            except requests.HTTPError as exc:
                error = f"of HTTP Error (maybe failed auth, maybe ill-formed post message, ...) using {__name__}"
                self.log.error(error)
                raise CannotInsertRunNumber(error) from exc
            except requests.ConnectionError as exc:
                error = f"connection to {self.API_SOCKET} wasn't successful using {__name__}"
                self.log.error(error)
                raise CannotInsertRunNumber(error) from exc
            except requests.Timeout as exc:
                error = f"connection to {self.API_SOCKET} timed out using {__name__}"
                self.log.error(error)
                raise CannotInsertRunNumber(error) from exc

        # can be removed if we use delete_on_close=False in f_tar
        os.remove(tar_name)
        return _input_data

    def post_drain_dataflow(self, _input_data, _context, **kwargs):
        try:
            requests.get(
                self.API_SOCKET + "/runregistry/updateStopTime/" + str(self.run_number),
                auth=(self.API_USER, self.API_PSWD),
                timeout=self.timeout,
            )

        except requests.HTTPError as exc:
            error = f"of HTTP Error (maybe failed auth, maybe ill-formed post message, ...) using {__name__}"
            self.log.error(error)
            raise CannotUpdateStopTime(error) from exc
        except requests.ConnectionError as exc:
            error = (
                f"connection to {self.API_SOCKET} wasn't successful using {__name__}"
            )
            self.log.error(error)
            raise CannotUpdateStopTime(error) from exc
        except requests.Timeout as exc:
            error = f"connection to {self.API_SOCKET} timed out using {__name__}"
            self.log.error(error)
            raise CannotUpdateStopTime(error) from exc
