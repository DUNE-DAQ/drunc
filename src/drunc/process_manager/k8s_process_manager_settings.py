from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypedDict

from drunc.utils.utils import get_logger


class ServiceConfig(TypedDict, total=False):
    headless_discovery_port: int


class PodManagementConfig(TypedDict, total=False):
    kill_timeout: int | float
    pod_ready_timeout: int | float


class CleanupConfig(TypedDict, total=False):
    restart_cleanup_time: int | float
    restart_cleanup_polling: int | float
    kill_watch_fallback_poll_interval: int | float


class CheckingConfig(TypedDict, total=False):
    watcher_retry_sleep: int
    pod_status_check_sleep: int
    host_cache_expiry: int
    service_startup_timeout: int
    socket_retry_timeout: int | float


class VolumeConfig(TypedDict, total=False):
    name: str
    mount_path: str
    host_path: str
    read_only: bool


class HostConfig(TypedDict, total=False):
    limits: dict[str, str]
    requests: dict[str, str]


def _empty_service_config() -> ServiceConfig:
    return ServiceConfig()


def _empty_pod_management_config() -> PodManagementConfig:
    return PodManagementConfig()


def _empty_cleanup_config() -> CleanupConfig:
    return CleanupConfig()


def _empty_checking_config() -> CheckingConfig:
    return CheckingConfig()


def _number_from_raw(key: str, raw: object) -> int | float:
    assert isinstance(raw, (int, float)), f"'{key}' must be numeric"
    return raw


def _int_from_raw(key: str, raw: object) -> int:
    assert isinstance(raw, int), f"'{key}' must be an integer"
    return raw


def _string_map_from_raw(key: str, raw: object) -> dict[str, str]:
    assert isinstance(raw, dict), f"'{key}' must be an object"
    return {str(k): str(v) for k, v in raw.items()}


def _service_config_from_raw(raw: object) -> ServiceConfig:
    assert isinstance(raw, dict), "'service' must be an object"
    config = ServiceConfig()
    value = raw.get("headless_discovery_port")
    if value is not None:
        config["headless_discovery_port"] = _int_from_raw(
            "service.headless_discovery_port", value
        )
    return config


def _pod_management_config_from_raw(raw: object) -> PodManagementConfig:
    assert isinstance(raw, dict), "'pod_management' must be an object"
    config = PodManagementConfig()
    value = raw.get("kill_timeout")
    if value is not None:
        config["kill_timeout"] = _number_from_raw("pod_management.kill_timeout", value)
    value = raw.get("pod_ready_timeout")
    if value is not None:
        config["pod_ready_timeout"] = _number_from_raw(
            "pod_management.pod_ready_timeout", value
        )
    return config


def _cleanup_config_from_raw(raw: object) -> CleanupConfig:
    assert isinstance(raw, dict), "'cleanup' must be an object"
    config = CleanupConfig()
    value = raw.get("restart_cleanup_time")
    if value is not None:
        config["restart_cleanup_time"] = _number_from_raw(
            "cleanup.restart_cleanup_time", value
        )
    value = raw.get("restart_cleanup_polling")
    if value is not None:
        config["restart_cleanup_polling"] = _number_from_raw(
            "cleanup.restart_cleanup_polling", value
        )
    value = raw.get("kill_watch_fallback_poll_interval")
    if value is not None:
        config["kill_watch_fallback_poll_interval"] = _number_from_raw(
            "cleanup.kill_watch_fallback_poll_interval", value
        )
    return config


def _volume_config_from_raw(raw: object) -> VolumeConfig:
    assert isinstance(raw, dict), "'volumes' entries must be objects"
    config = VolumeConfig()
    name = raw.get("name")
    if name is not None:
        assert isinstance(name, str), "'volumes.name' must be a string"
        config["name"] = name
    mount_path = raw.get("mount_path")
    if mount_path is not None:
        assert isinstance(mount_path, str), "'volumes.mount_path' must be a string"
        config["mount_path"] = mount_path
    host_path = raw.get("host_path")
    if host_path is not None:
        assert isinstance(host_path, str), "'volumes.host_path' must be a string"
        config["host_path"] = host_path
    value = raw.get("read_only")
    if value is not None:
        assert isinstance(value, bool), "'volumes.read_only' must be boolean"
        config["read_only"] = value
    return config


def _checking_config_from_raw(raw: object) -> CheckingConfig:
    assert isinstance(raw, dict), "'checking' must be an object"
    config = CheckingConfig()
    value = raw.get("watcher_retry_sleep")
    if value is not None:
        config["watcher_retry_sleep"] = _int_from_raw(
            "checking.watcher_retry_sleep", value
        )
    value = raw.get("pod_status_check_sleep")
    if value is not None:
        config["pod_status_check_sleep"] = _int_from_raw(
            "checking.pod_status_check_sleep", value
        )
    value = raw.get("host_cache_expiry")
    if value is not None:
        config["host_cache_expiry"] = _int_from_raw("checking.host_cache_expiry", value)
    value = raw.get("service_startup_timeout")
    if value is not None:
        config["service_startup_timeout"] = _int_from_raw(
            "checking.service_startup_timeout", value
        )
    value = raw.get("socket_retry_timeout")
    if value is not None:
        config["socket_retry_timeout"] = _number_from_raw(
            "checking.socket_retry_timeout", value
        )
    return config


def _host_config_from_raw(raw: object) -> HostConfig:
    assert isinstance(raw, dict), "'host_configs' entries must be objects"
    config = HostConfig()
    limits = raw.get("limits")
    if limits is not None:
        config["limits"] = _string_map_from_raw("host_configs.limits", limits)
    requests = raw.get("requests")
    if requests is not None:
        config["requests"] = _string_map_from_raw("host_configs.requests", requests)
    return config


@dataclass
class K8sProcessManagerSettings:
    """Typed settings for the K8s process manager."""

    labels: dict[str, str] = field(default_factory=dict)
    readout_app_selector: str = "runp"
    service: ServiceConfig = field(default_factory=_empty_service_config)
    pod_management: PodManagementConfig = field(
        default_factory=_empty_pod_management_config
    )
    cleanup: CleanupConfig = field(default_factory=_empty_cleanup_config)
    volumes: list[VolumeConfig] = field(default_factory=list)
    home_path_base: str | None = None
    checking: CheckingConfig = field(default_factory=_empty_checking_config)
    host_configs: dict[str, HostConfig] = field(default_factory=dict)
    extra: dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: object) -> "K8sProcessManagerSettings":
        if raw is None:
            return cls()
        assert isinstance(raw, dict), "'settings' must be an object"

        settings = cls()
        known_fields = {
            "labels",
            "readout_app_selector",
            "service",
            "pod_management",
            "cleanup",
            "volumes",
            "home_path_base",
            "checking",
            "host_configs",
        }

        for key, value in raw.items():
            if key not in known_fields:
                settings.extra[key] = value
                get_logger("process_manager.config_validation").warning(
                    "Discarding unsupported K8s process manager setting '%s'",
                    key,
                )
                continue

            if key == "labels":
                settings.labels = _string_map_from_raw("labels", value)
            elif key == "readout_app_selector":
                assert isinstance(value, str), "'readout_app_selector' must be a string"
                settings.readout_app_selector = value
            elif key == "service":
                settings.service = _service_config_from_raw(value)
            elif key == "pod_management":
                settings.pod_management = _pod_management_config_from_raw(value)
            elif key == "cleanup":
                settings.cleanup = _cleanup_config_from_raw(value)
            elif key == "volumes":
                assert isinstance(value, list), "'volumes' must be a list"
                settings.volumes = [_volume_config_from_raw(v) for v in value]
            elif key == "home_path_base":
                assert isinstance(value, str), "'home_path_base' must be a string"
                settings.home_path_base = value
            elif key == "checking":
                settings.checking = _checking_config_from_raw(value)
            elif key == "host_configs":
                assert isinstance(value, dict), "'host_configs' must be an object"
                settings.host_configs = {
                    str(host): _host_config_from_raw(config)
                    for host, config in value.items()
                }

        return settings

    def get(self, key: str, default: object | None = None) -> object | None:
        if key == "labels":
            return self.labels
        if key == "readout_app_selector":
            return self.readout_app_selector
        if key == "service":
            return self.service
        if key == "pod_management":
            return self.pod_management
        if key == "cleanup":
            return self.cleanup
        if key == "volumes":
            return self.volumes
        if key == "home_path_base":
            return self.home_path_base
        if key == "checking":
            return self.checking
        if key == "host_configs":
            return self.host_configs
        return self.extra.get(key, default)
