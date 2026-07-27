import json
import os
import sys
from enum import Enum
from importlib import resources
from typing import Protocol, cast
from urllib.parse import unquote, urlparse

from jsonschema import ValidationError
from jsonschema import validate as js_validate
from kafkaopmon.OpMonPublisher import (
    OpMonPublisher as KafkaOpMonPublisher,
)
from opmonlib.publisher import OpMonPublisher
from opmonlib.utils import parse_opmon_conf

from drunc.exceptions import DruncCommandException
from drunc.process_manager.exceptions import UnknownProcessManagerType
from drunc.utils.configuration import ConfHandler
from drunc.utils.utils import get_logger, touch_and_chmod


class _RunsOnInner(Protocol):
    id: str


class _RunsOn(Protocol):
    runs_on: _RunsOnInner


class _Service(Protocol):
    id: str
    port: int
    protocol: str


class _SessionDal(Protocol):
    id: str
    controller_log_level: str


class _AppLike(Protocol):
    id: str
    runs_on: _RunsOn
    exposes_service: list[_Service]

    def oksTypes(self) -> list[str]: ...


class _OpMonConf(Protocol):
    path: str
    session: str
    application: str
    opmon_type: str


PROCESS_SHUTDOWN_ORDERING = [
    "unknown",
    "application",
    "segment-controller",
    "root-controller",
    "infrastructure-applications",
]


class ProcessManagerTypes(Enum):
    Unknown = 0
    SSH_SHELL = 1
    K8s = 2
    SSH_PARAMIKO = 3


class ProcessManagerConfData:
    def __init__(self) -> None:
        self.authoriser: object | None = None
        self.type = ProcessManagerTypes.Unknown
        self.command_address = ""
        self.environment = {}
        self.settings = {}
        self.opmon_uri = None
        self.opmon_publisher = None


class ProcessManagerConfHandler(ConfHandler):
    @staticmethod
    def _coerce_timeout(value: object, default: float = 0.5) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return default
        return default

    def __init__(self, log_path: str, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.log_path = log_path
        self.log = get_logger("process_manager.conf_handler")

    def get_log_path(self) -> str:
        return self.log_path

    def _parse_dict(self, data: dict[str, object]) -> ProcessManagerConfData:
        new_data = ProcessManagerConfData()
        new_data.environment = cast(dict[str, str], data.get("environment", {}))
        new_data.settings = cast(dict[str, object], data.get("settings", {}))

        conf_type = str(data["type"]).lower()

        match conf_type:
            case "ssh":
                new_data.type = ProcessManagerTypes.SSH_SHELL
                new_data.kill_timeout = self._coerce_timeout(
                    data.get("kill_timeout", 0.5)
                )
            case "ssh-paramiko":
                new_data.type = ProcessManagerTypes.SSH_PARAMIKO
                new_data.kill_timeout = self._coerce_timeout(
                    data.get("kill_timeout", 0.5)
                )
            case "k8s":
                new_data.type = ProcessManagerTypes.K8s
                new_data.image = str(data.get("image", "ghcr.io/dune-daq/alma9:latest"))
            case _:
                raise UnknownProcessManagerType(str(data["type"]))

        # opmon_publisher left as default None
        self.opmon_conf = cast(
            _OpMonConf,
            parse_opmon_conf(
                log=self.log,
                conf=data.get("opmon_conf", None),
                uri=data.get("opmon_uri", None),
                session=new_data.type.name,
                application="process_manager",
            ),
        )

        if self.opmon_conf.path == "./info.json":
            self.opmon_conf.path = (
                "./info."
                + self.opmon_conf.session
                + "."
                + self.opmon_conf.application
                + ".json"
            )

        self.log.debug(
            "Initializing process manager OpMon with configuration %s", self.opmon_conf
        )
        try:
            if self.opmon_conf.opmon_type == "stream":
                self.opmon_publisher = KafkaOpMonPublisher(self.opmon_conf)
                self.log.debug(
                    "KafkaOpMonPublisher initialized with configuration %s",
                    self.opmon_conf,
                )
            else:
                touch_and_chmod(self.opmon_conf.path)
                self.opmon_publisher = OpMonPublisher(
                    conf=self.opmon_conf, rich_handler=True
                )
                self.log.debug(
                    "%s OpMonPublisher initialized with configuration %s",
                    self.opmon_conf.opmon_type,
                    self.opmon_conf,
                )

        except Exception as e:
            self.log.error("Failed to initialize OpMonPublisher: %s", e)
            raise DruncCommandException("Failed to initialize OpMonPublisher.")


def get_commandline_parameters(
    config_filename: str,
    session_dal: _SessionDal,
    session_name: str,
    obj: _AppLike,
) -> list[str]:
    """
    Build CLI arguments used to launch a process manager application.

    The control endpoint is derived from the first service exposed by ``obj`` whose
    id ends with ``"_control"``. If no such service is present, defaults are kept
    (empty protocol and port ``-1``), matching existing behavior.

    Args:
        config_filename: OKS configuration filename/path passed to ``-d``.
        session_dal: Session DAL object containing session metadata and log level.
        session_name: Runtime session name passed to ``-s``.
        obj: DAL object (application/controller) being launched.

    Returns:
        Ordered list of command-line arguments for process launch.
    """
    runs_on = obj.runs_on.runs_on.id
    control_service_port = -1
    control_service_protocol = ""
    for svc in obj.exposes_service:
        if svc.id.endswith("_control"):
            control_service_port = svc.port
            control_service_protocol = svc.protocol
            break

    commandline_parameters = [
        "-s",
        session_name,
        "-k",
        session_dal.id,
        "-n",
        obj.id,
        "-c",
        f"{control_service_protocol}://{runs_on}:{control_service_port}",
        "-d",
        config_filename,
    ]
    if "RCApplication" in obj.oksTypes():
        commandline_parameters += [
            "-l",
            session_dal.controller_log_level,
        ]

    return commandline_parameters


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
        resource_path = resources.files("drunc.data.process_manager")
        packaged_configurations = [p.name for p in resource_path.iterdir()]
        if process_manager_conf_filename in packaged_configurations:
            process_manager_conf_filename = "file://" + str(
                resource_path / process_manager_conf_filename
            )

        else:
            log = get_logger("process_manager.ConfHandler")
            log.error(
                f"Configuration [red]{process_manager_conf_filename}[/red] not found, check filename spelling or use a packaged configuration as one of [green]{'[/green], [green]'.join(packaged_configurations)}[/green]."
            )
            sys.exit(1)
    return process_manager_conf_filename


def _load_pm_schema_from_package() -> dict[str, object]:
    """Load JSON Schema from packaged file; raise if missing or unreadable."""
    try:
        # Package path for schema JSON: drunc/data/process_manager/schema/process_manager.schema.json
        schema_resource = (
            resources.files("drunc.data.process_manager.schema")
            / "process_manager.schema.json"
        )
        if not hasattr(schema_resource, "open"):
            raise FileNotFoundError("process_manager.schema.json resource not found")
        with schema_resource.open("r", encoding="utf-8") as f:
            return cast(dict[str, object], json.load(f))
    except Exception as e:
        logger = get_logger("process_manager.config_validation")
        logger.error(f"Failed to load packaged schema: {e}")
        raise DruncCommandException(
            "Packaged process manager schema could not be loaded."
        )


def _load_config_from_source(source: str | dict[str, object]) -> dict[str, object]:
    """
    Accepts:
      - file URLs (file:///...),
      - plain filesystem paths,
      - raw JSON text (starts with '{' or '[').
    Returns dict.
    """
    if isinstance(source, str):
        s = source.lstrip()
        # Raw JSON text
        if s.startswith("{") or s.startswith("["):
            return cast(dict[str, object], json.loads(source))

        # URL?
        if "://" in source:
            u = urlparse(source)
            if u.scheme == "file":
                path = unquote(u.path)
                with open(path, "r", encoding="utf-8") as f:
                    return cast(dict[str, object], json.load(f))
            raise FileNotFoundError(f"Unsupported URL scheme: {u.scheme}")

        # Name or filesystem path: resolve and then load
        resolved_url = get_process_manager_configuration(source)
        u = urlparse(resolved_url)
        path = unquote(u.path)
        with open(path, "r", encoding="utf-8") as f:
            return cast(dict[str, object], json.load(f))

    # Already a dict
    elif isinstance(source, dict):
        return source

    raise TypeError("validate_config() expects dict, path, URL, or raw JSON text")


def validate_pm_config(config_or_source: str | dict[str, object]) -> bool:
    try:
        pm_conf = _load_config_from_source(config_or_source)
        schema = _load_pm_schema_from_package()

        js_validate(instance=pm_conf, schema=schema)

        return True

    except ValidationError as e:
        logger = get_logger("process_manager.config_validation")
        logger.error(e.message)
        logger.error(list(e.path))
        return False
    except (OSError, json.JSONDecodeError, FileNotFoundError, TypeError) as e:
        logger = get_logger("process_manager.config_validation")
        logger.error(f"Failed to read/parse config: {e}")
        return False
