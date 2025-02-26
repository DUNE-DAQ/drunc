import os
from enum import Enum
from importlib.resources import path
from urllib.parse import urlparse

from appmodel import smart_daq_application_construct_commandline_parameters
from confmodel import (
    daq_application_construct_commandline_parameters,
    rc_application_construct_commandline_parameters,
)

from drunc.broadcast.server.configuration import KafkaBroadcastSenderConfData
from drunc.process_manager.exceptions import UnknownProcessManagerType
from drunc.utils.configuration import ConfHandler
from drunc.utils.utils import get_logger


class ProcessManagerTypes(Enum):
    Unknown = 0
    SSH = 1
    K8s = 2


class ProcessManagerConfData:
    def __init__(self):
        self.broadcaster = None
        self.authoriser = None
        self.type = ProcessManagerTypes.Unknown
        self.command_address = ""


class ProcessManagerConfHandler(ConfHandler):
    def __init__(self, log_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_path = log_path
        self.log = get_logger("process_manager.conf_handler")

    def _parse_dict(self, data):
        new_data = ProcessManagerConfData()
        if data.get("broadcaster"):
            new_data.broadcaster = KafkaBroadcastSenderConfData.from_dict(
                data.get("broadcaster")
            )
        else:
            new_data.broadcaster = None
        new_data.authoriser = None

        match data["type"].lower():
            case "ssh":
                new_data.type = ProcessManagerTypes.SSH
                new_data.kill_timeout = data.get("kill_timeout", 0.5)
            case "k8s":
                new_data.type = ProcessManagerTypes.K8s
                new_data.image = data.get("image", "ghcr.io/dune-daq/alma9:latest")
            case _:
                raise UnknownProcessManagerType(data["type"])

        return new_data

    def create_id(self, obj, segment=None, **kwargs):
        if hasattr(obj, "oksTypes"):
            if "RCApplication" in obj.oksTypes():
                if segment.segments:
                    self.root_id += 1
                    self.controller_id = 0
                    self.process_id = 0
                    self.process_id_infra = 0
                    id = f"{self.root_id}.{self.controller_id}.{self.process_id}"
                    return id
                elif not segment.segments:
                    self.controller_id += 1
                    self.process_id = 0
                    id = f"{self.root_id}.{self.controller_id}.{self.process_id}"
                    return id
            elif "SmartDaqApplication" in obj.oksTypes():
                self.process_id += 1
                id = f"{self.root_id}.{self.controller_id}.{self.process_id}"
                return id
            else:
                self.process_id_infra += 1
                id = f"{self.root_id}.0.{self.process_id_infra}"
                return id


def get_cla(db, session_uid, obj):
    if hasattr(obj, "oksTypes"):
        if "RCApplication" in obj.oksTypes():
            return rc_application_construct_commandline_parameters(
                db, session_uid, obj.id
            )

        elif "SmartDaqApplication" in obj.oksTypes():
            return smart_daq_application_construct_commandline_parameters(
                db, session_uid, obj.id
            )

        elif "DaqApplication" in obj.oksTypes():
            return daq_application_construct_commandline_parameters(
                db, session_uid, obj.id
            )

    return obj.commandline_parameters


def get_process_manager_configuration(process_manager_conf_filename: str) -> str:
    ## Make the configuration name finding easier
    if os.path.splitext(process_manager_conf_filename)[1] != ".json":
        process_manager_conf_filename += ".json"
    ## If no scheme is provided, assume that it is an internal packaged configuration.
    ## First check it's not an existing external file
    if os.path.isfile(process_manager_conf_filename):
        if urlparse(process_manager_conf_filename).scheme == "":
            process_manager_conf_filename = "file://" + process_manager_conf_filename
    else:
        ## Check if the file is in the list of packaged configurations
        packaged_configurations = os.listdir(path("drunc.data.process_manager", ""))
        if process_manager_conf_filename in packaged_configurations:
            process_manager_conf_filename = (
                "file://"
                + str(path("drunc.data.process_manager", ""))
                + "/"
                + process_manager_conf_filename
            )
        else:
            log = get_logger("process_manager.ConfHandler")
            log.error(
                f"Configuration [red]{process_manager_conf_filename}[/red] not found, check filename spelling or use a packaged configuration as one of [green]{'[/green], [green]'.join(packaged_configurations)}[/green]."
            )
            exit()
    return process_manager_conf_filename
