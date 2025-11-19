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
    DBRunRegistryConfigurationError,
    DotDruncJsonIncorrectFormat,
)
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
        """
        Upload a copy of the XML and JSON configurations used to take the current run to
        the Run Registry, and insert a new run number entry.

        Args:
            _input_data (dict): Input data dictionary containing run information.
            _context: Context object containing configuration and state information.
            **kwargs: Additional keyword arguments.

        Returns:
            dict: The input data dictionary, unchanged.

        Raises:
            CannotGetSoftwareVersion: If the software version cannot be determined.
            CannotInsertRunNumber: If there is an error inserting the run number into
                the Run Registry
        """

        # Seems like run_number isn't in _input_data in post_drain_dataflow so need to
        # initialise it here
        self.run_number = _input_data[
            "run"
        ]

        # Get the environment variables for upload
        software_version = os.getenv("DUNE_DAQ_BASE_RELEASE")
        if software_version == None:
            raise CannotGetSoftwareVersion()

        # Get the input data variables for upload
        run_type = _input_data.get("production_vs_test", "TEST")

        # Get the configuration variables for upload
        conf = _context.configuration
        det_id = conf.db.get_dal(
            class_name="Session", uid=_context.configuration.oks_key.session
        ).detector_configuration.id

        # Create a consolidated XML configuration file
        xml_file = tempfile.NamedTemporaryFile(suffix=".data.xml", delete=False)
        xml_filename = xml_file.name
        try:
            consolidate_db(_context.configuration.initial_data.split(":")[1], xml_filename)
        except RuntimeError as exc:
            error = "while consolidating the configuration database to XML format using consolidate_db"
            self.log.error(error)
            raise DBRunRegistryConfigurationError(error) from exc

        # Create a JSON configuration file
        json_file = tempfile.NamedTemporaryFile(suffix=".data.json", delete=False)
        json_filename = json_file.name
        try:
            jsonify_xml_data(xml_filename, json_filename)
        except RuntimeError as exc:
            error = "while converting XML configuration to JSON format using jsonify_xml_data"
            self.log.error(error)
            raise DBRunRegistryConfigurationError(error) from exc

        # Create a entry point file (contains the session key)
        entry_point_file = tempfile.NamedTemporaryFile(
            suffix="_entry_point.txt", delete=False
        )
        entry_point_filename = entry_point_file.name
        with open(entry_point_filename, "w") as f:
            f.write(conf.oks_key.session)

        tarball = tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False)
        tarball_name = tarball.name

        with tarfile.open(fileobj=tarball, mode="w:gz") as tar:
            tar.add(xml_filename, arcname=os.path.basename(xml_filename))
            tar.add(json_filename, arcname=os.path.basename(json_filename))
            tar.add(entry_point_filename, arcname=os.path.basename(entry_point_filename))
        # f_tar.close()

        
        # Publish to the run registry
        with open(tarball_name, "rb") as f:
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

        # Clean up
        for file in [xml_filename, json_filename, entry_point_filename, tarball_name]:
            os.remove(file)

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
