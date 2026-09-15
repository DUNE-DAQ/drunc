from __future__ import annotations

from dataclasses import dataclass, field

from drunc.utils.utils import get_logger


@dataclass
class SSHProcessManagerSettings:
    """Typed settings for the SSH process manager."""

    disable_localhost_host_key_check: bool = False
    disable_host_key_check: bool = False
    extra: dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: object) -> "SSHProcessManagerSettings":
        if raw is None:
            return cls()
        assert isinstance(raw, dict), "'settings' must be an object"

        settings = cls()
        for key, value in raw.items():
            if key not in {
                "disable_localhost_host_key_check",
                "disable_host_key_check",
            }:
                settings.extra[key] = value
                get_logger("process_manager.config_validation").warning(
                    "Discarding unsupported SSH process manager setting '%s'",
                    key,
                )
                continue

            if key == "disable_localhost_host_key_check":
                assert isinstance(value, bool), (
                    "'disable_localhost_host_key_check' must be boolean"
                )
                settings.disable_localhost_host_key_check = value
            elif key == "disable_host_key_check":
                assert isinstance(value, bool), (
                    "'disable_host_key_check' must be boolean"
                )
                settings.disable_host_key_check = value

        return settings

    def get(self, key: str, default: object | None = None) -> object | None:
        if key == "disable_localhost_host_key_check":
            return self.disable_localhost_host_key_check
        if key == "disable_host_key_check":
            return self.disable_host_key_check
        return self.extra.get(key, default)
