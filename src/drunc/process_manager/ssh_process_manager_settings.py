from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeVar

from drunc.exceptions import DruncSetupException
from drunc.utils.utils import get_logger

T = TypeVar("T")


def require_type(value: object, expected_type: type[T], message: str) -> T:
    """Return `value` narrowed to `expected_type`, or raise DruncSetupException."""
    if not isinstance(value, expected_type):
        raise DruncSetupException(message)
    return value


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
        raw = require_type(raw, dict, "'settings' must be a JSON object")

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
                settings.disable_localhost_host_key_check = require_type(
                    value, bool, "'disable_localhost_host_key_check' must be boolean"
                )
            elif key == "disable_host_key_check":
                settings.disable_host_key_check = require_type(
                    value, bool, "'disable_host_key_check' must be boolean"
                )

        return settings
