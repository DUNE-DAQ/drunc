import os
from enum import Enum
from importlib.resources import path
from urllib.parse import urlparse

from drunc_core.broadcast.server.configuration import KafkaBroadcastSenderConfData
from drunc_core.exceptions import DruncCommandException
from drunc_core.utils.configuration import ConfHandler
from drunc_core.utils.utils import get_logger
from kafkaopmon.OpMonPublisher import OpMonPublisher

from drunc.process_orchestrator.exceptions import UnknownProcessOrchestratorType


class ProcessOrchestratorTypes(Enum):
    Unknown = 0
    SSH = 1
    K8s = 2


class ProcessOrchestratorConfData:
    def __init__(self):
        self.broadcaster = None
        self.authoriser = None
        self.type = ProcessOrchestratorTypes.Unknown
        self.command_address = ""
        self.environment = {}
        self.opmon_uri = None
        self.opmon_publisher = None


class ProcessOrchestratorConfHandler(ConfHandler):
    def __init__(self, log_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_path = log_path
        self.log = get_logger("process_orchestrator.conf_handler")

    def _parse_dict(self, data):
        new_data = ProcessOrchestratorConfData()
        if data.get("broadcaster"):
            new_data.broadcaster = KafkaBroadcastSenderConfData.from_dict(
                data.get("broadcaster")
            )
        else:
            new_data.broadcaster = None
        new_data.authoriser = None
        new_data.environment = data.get("environment", {})

        match data["type"].lower():
            case "ssh":
                new_data.type = ProcessOrchestratorTypes.SSH
                new_data.kill_timeout = data.get("kill_timeout", 0.5)
            case "k8s":
                new_data.type = ProcessOrchestratorTypes.K8s
                new_data.image = data.get("image", "ghcr.io/dune-daq/alma9:latest")
            case _:
                raise UnknownProcessOrchestratorType(data["type"])

        new_data.opmon_publisher = None
        opmon_uri = data.get("opmon_uri", None)

        if not opmon_uri:
            self.log.info("Missing 'opmon_uri' in configuration.")
            return new_data

        opmon_path = opmon_uri.get("path", "")
        opmon_type = opmon_uri.get("type", "")
        new_data.opmon_sleep_time = opmon_uri.get("sleep_time", 5.0)

        if not opmon_path or not opmon_type:
            self.log.error("Invalid 'opmon_uri' format: Missing required fields.")
            raise DruncCommandException(
                "Invalid 'opmon_uri' format: Missing required fields."
            )

        self.log.info(
            f"OpMon path {opmon_path} and type {opmon_type} is enabled, sleep time: {new_data.opmon_sleep_time} s"
        )

        if "/" in opmon_path:
            opmon_bootstrap, opmon_topic = opmon_path.split("/", 1)
        else:
            opmon_bootstrap = opmon_path
            opmon_topic = "opmon_stream"

        if opmon_type == "stream":
            try:
                new_data.opmon_publisher = OpMonPublisher(
                    default_topic=opmon_topic, bootstrap=opmon_bootstrap
                )
                self.log.info(
                    f"OpMonPublisher initialized: {opmon_bootstrap}/{opmon_topic}"
                )

            except Exception as e:
                self.log.error(f"Failed to initialize OpMonPublisher: {e}")
                raise DruncCommandException("Failed to initialize OpMonPublisher.")
        else:
            self.log.error(f"Unsupported OpMon type: {opmon_type}")
            raise DruncCommandException(f"Unsupported OpMon type: {opmon_type}")

        return new_data


def get_process_orchestrator_configuration(
    process_orchestrator_conf_filename: str,
) -> str:
    ## Make the configuration name finding easier
    if os.path.splitext(process_orchestrator_conf_filename)[1] != ".json":
        process_orchestrator_conf_filename += ".json"
    ## If no scheme is provided, assume that it is an internal packaged configuration.
    ## First check it's not an existing external file
    if os.path.isfile(process_orchestrator_conf_filename):
        if urlparse(process_orchestrator_conf_filename).scheme == "":
            process_orchestrator_conf_filename = (
                "file://" + process_orchestrator_conf_filename
            )
    else:
        ## Check if the file is in the list of packaged configurations
        packaged_configurations = os.listdir(
            path("drunc.data.process_orchestrator", "")
        )
        if process_orchestrator_conf_filename in packaged_configurations:
            process_orchestrator_conf_filename = (
                "file://"
                + str(path("drunc.data.process_orchestrator", ""))
                + "/"
                + process_orchestrator_conf_filename
            )
        else:
            log = get_logger("process_orchestrator.ConfHandler")
            log.error(
                f"Configuration [red]{process_orchestrator_conf_filename}[/red] not found, check filename spelling or use a packaged configuration as one of [green]{'[/green], [green]'.join(packaged_configurations)}[/green]."
            )
            exit()
    return process_orchestrator_conf_filename
