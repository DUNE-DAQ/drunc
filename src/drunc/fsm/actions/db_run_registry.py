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
# from drunc.utils.utils import get_logger

import logging
from datetime import datetime
from shutil import copy2

class DBRunRegistry(FSMAction):
    def __init__(self, configuration):
        super().__init__(name="db-run-registry")
        self.log = logging.getLogger("drunc.controller.usvc_db_run_registry")

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        self.log.addHandler(handler)
        self.log.setLevel(logging.DEBUG)

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
        Publish the configuration as both an XML and JSON file to the Run Registry prior
        to starting the run.

        Args:
            _input_data (dict): Input data dictionary containing run information.
            _context: Context object providing access to configuration and database.
        
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

        # Get the environment variables that need to be published
        software_version = os.getenv("DUNE_DAQ_BASE_RELEASE")
        if software_version == None:
            raise CannotGetSoftwareVersion()

        # Get the metadata from the input data
        run_type = _input_data.get("production_vs_test", "TEST")

        # Get the detector configuration ID from the configuration file
        controller_config = _context.configuration
        det_id = controller_config.db.get_dal(
            class_name="Session", uid=_context.configuration.oks_key.session
        ).detector_configuration.id

        # Create a temporary file for the XML configuration file
        f_xml = tempfile.NamedTemporaryFile(suffix=".data.xml", delete=True)
        xml_name = f_xml.name
        session_file_path = controller_config.initial_data.replace("oksconflibs:", "")
        self.log.critical(f"PP: Consolidating DB with {session_file_path=} and {xml_name=}")
        consolidate_db(session_file_path, xml_name)

        # Create a timestamped copy of the XML file in /tmp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_xml_path = f"/tmp/DEBUGGING_{timestamp}.data.xml"
        copy2(xml_name, timestamped_xml_path)
        self.log.critical(f"Created timestamped XML copy at {timestamped_xml_path}")

        # Create a temporary file for the JSON configuration file
        f_json = tempfile.NamedTemporaryFile(suffix=".data.json", delete=True)
        f_entry_point = tempfile.NamedTemporaryFile(
            suffix="_entry_point.txt", delete=True
        )
        json_name = f_json.name

        with open(xml_name, "r") as xml_file:
            for line in xml_file:
                self.log.critical(line.rstrip())

        jsonify_xml_data(xml_name, json_name)

        # Create a temporary file for the entry point file
        # (only contains the session key)
        entry_point_name = f_entry_point.name
        with open(entry_point_name, "w") as f:
            f.write(_context.configuration.oks_key.session)

        # Create a tar.gz file containing the XML, JSON, and entry point files
        f_tar = tempfile.NamedTemporaryFile(
            suffix=".tar.gz",
            delete=False,  # delete when f_tar gets out of scope
            # for after python 3.12...
            # delete = True, # delete when f_tar gets out of scope
            # delete_on_close = False
        )
        tar_name = f_tar.name

        # Write the files to the tar.gz archive
        with tarfile.open(fileobj=f_tar, mode="w:gz") as tar:
            tar.add(xml_name, arcname=os.path.basename(xml_name))
            tar.add(json_name, arcname=os.path.basename(json_name))
            tar.add(entry_point_name, arcname=os.path.basename(entry_point_name))
        f_tar.close()

        # Post the tar.gz file to the Run Registry API
        # with open(tar_name, "rb") as f:
        #     files = {"file": f}
        #     post_data = {
        #         "run_num": self.run_number,
        #         "det_id": det_id,
        #         "run_type": run_type,
        #         "software_version": software_version,
        #     }

        #     try:
        #         r = requests.post(
        #             self.API_SOCKET + "/runregistry/insertRun/",
        #             files=files,
        #             data=post_data,
        #             auth=(self.API_USER, self.API_PSWD),
        #             timeout=self.timeout,
        #         )
        #         r.raise_for_status()
        #     except requests.HTTPError as exc:
        #         error = f"of HTTP Error (maybe failed auth, maybe ill-formed post message, ...) using {__name__}"
        #         self.log.error(error)
        #         raise CannotInsertRunNumber(error) from exc
        #     except requests.ConnectionError as exc:
        #         error = f"connection to {self.API_SOCKET} wasn't successful using {__name__}"
        #         self.log.error(error)
        #         raise CannotInsertRunNumber(error) from exc
        #     except requests.Timeout as exc:
        #         error = f"connection to {self.API_SOCKET} timed out using {__name__}"
        #         self.log.error(error)
        #         raise CannotInsertRunNumber(error) from exc

        # Can be removed if we use delete_on_close=False in f_tar
        os.remove(tar_name)

        # Clean up temporary files  
        f_xml.close()
        f_json.close()
        f_entry_point.close()

        # Validate that the files were cleaned up
        for temp_file in [xml_name, json_name, entry_point_name]:
            if os.path.exists(temp_file):
                err_msg = f"Temporary file {temp_file} was not deleted."
                raise OSError(err_msg)

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
