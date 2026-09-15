from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias, TypedDict

from drunc.process_manager.exceptions import UnknownProcessManagerType
from drunc.process_manager.k8s_process_manager_settings import K8sProcessManagerSettings
from drunc.process_manager.ssh_process_manager_settings import SSHProcessManagerSettings


class OpMonConfig(TypedDict, total=False):
    """Subset of the OpMon JSON config actually parsed by the PM implementation."""

    level: str
    interval_s: int | float
    path: str
    type: str


class OpMonUri(TypedDict):
    path: str
    type: str


JsonObject: TypeAlias = dict[str, object]
ProcessManagerSettings: TypeAlias = (
    SSHProcessManagerSettings | K8sProcessManagerSettings
)


def _settings_from_raw(pm_type: str, raw: object) -> ProcessManagerSettings:
    if pm_type in {"ssh", "ssh-paramiko"}:
        return SSHProcessManagerSettings.from_raw(raw)
    if pm_type == "k8s":
        return K8sProcessManagerSettings.from_raw(raw)
    raise UnknownProcessManagerType(pm_type)


def _opmon_config_from_raw(raw: object) -> OpMonConfig | None:
    if raw is None:
        return None
    assert isinstance(raw, dict), "'opmon_conf' must be an object or null"
    config = OpMonConfig()

    level = raw.get("level")
    if level is not None:
        assert isinstance(level, str), "'opmon_conf.level' must be a string"
        config["level"] = level

    interval_s = raw.get("interval_s")
    if interval_s is not None:
        assert isinstance(interval_s, (int, float)), (
            "'opmon_conf.interval_s' must be numeric"
        )
        config["interval_s"] = interval_s

    path = raw.get("path")
    if path is not None:
        assert isinstance(path, str), "'opmon_conf.path' must be a string"
        config["path"] = path

    opmon_type = raw.get("type")
    if opmon_type is not None:
        assert isinstance(opmon_type, str), "'opmon_conf.type' must be a string"
        config["type"] = opmon_type

    return config


def _opmon_uri_from_raw(raw: object) -> OpMonUri | None:
    if raw is None:
        return None
    assert isinstance(raw, dict), "'opmon_uri' must be an object or null"
    path = raw.get("path")
    opmon_type = raw.get("type")
    assert isinstance(path, str), "'opmon_uri.path' must be a string"
    assert isinstance(opmon_type, str), "'opmon_uri.type' must be a string"
    return {"path": path, "type": opmon_type}


def json_object_from_raw(raw: object) -> JsonObject:
    assert isinstance(raw, dict), "JSON input must be an object"
    return {str(key): value for key, value in raw.items()}


@dataclass
class ProcessManagerJsonConfData:
    """Typed representation of a process manager JSON configuration."""

    type: str
    settings: ProcessManagerSettings
    environment: dict[str, str] = field(default_factory=dict)
    opmon_conf: OpMonConfig | None = None
    opmon_uri: OpMonUri | None = None
    kill_timeout: float = 0.5
    image: str = "ghcr.io/dune-daq/alma9:latest"

    @classmethod
    def from_raw(cls, raw: object) -> "ProcessManagerJsonConfData":
        assert isinstance(raw, dict), "process manager config must be a JSON object"

        pm_type = raw.get("type")
        assert isinstance(pm_type, str), "'type' must be a string"

        environment_raw = raw.get("environment", {})
        assert isinstance(environment_raw, dict), "'environment' must be a dict"
        environment: dict[str, str] = {
            str(k): str(v) for k, v in environment_raw.items()
        }

        settings_raw = raw.get("settings", {})
        settings = _settings_from_raw(pm_type, settings_raw)
        opmon_conf = _opmon_config_from_raw(raw.get("opmon_conf"))
        opmon_uri = _opmon_uri_from_raw(raw.get("opmon_uri"))

        kill_timeout_raw = raw.get("kill_timeout", 0.5)
        assert isinstance(kill_timeout_raw, (int, float)), (
            "'kill_timeout' must be numeric"
        )

        image_raw = raw.get("image", "ghcr.io/dune-daq/alma9:latest")
        assert isinstance(image_raw, str), "'image' must be a string"

        return cls(
            type=pm_type,
            environment=environment,
            settings=settings,
            opmon_conf=opmon_conf,
            opmon_uri=opmon_uri,
            kill_timeout=float(kill_timeout_raw),
            image=image_raw,
        )
