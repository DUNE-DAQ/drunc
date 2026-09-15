from unittest.mock import Mock

import pytest

from drunc.process_manager import ssh_process_manager_settings
from drunc.process_manager.configuration import ProcessManagerTypes
from drunc.process_manager.ssh_process_manager_settings import SSHProcessManagerSettings
from tests.process_manager.conftest import ProcessManagerFromJson


def test_process_manager_from_json_loads_ssh_settings(
    process_manager_from_json: ProcessManagerFromJson,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = Mock()
    monkeypatch.setattr(
        ssh_process_manager_settings, "get_logger", Mock(return_value=logger)
    )
    config_data: dict[str, object] = {
        "type": "ssh",
        "environment": {"APP_ENV": "test", "RETRIES": 3},
        "settings": {
            "disable_localhost_host_key_check": True,
            "disable_host_key_check": False,
            "discarded_ssh_setting": "unused",
        },
        "opmon_conf": {
            "level": "INFO",
            "interval_s": 1.5,
            "path": "/tmp/ssh-opmon.json",
            "type": "file",
        },
        "opmon_uri": {"path": "/tmp/ssh-opmon-uri.json", "type": "file"},
        "kill_timeout": 4.25,
    }

    handler, logger, opmon_conf, publisher = process_manager_from_json(
        config_data, logger
    )

    assert handler.initial_data.type == "ssh"
    assert handler.initial_data.environment == {"APP_ENV": "test", "RETRIES": "3"}
    assert handler.environment == {"APP_ENV": "test", "RETRIES": "3"}
    assert handler.pm_type is ProcessManagerTypes.SSH_SHELL
    assert handler.kill_timeout == 4.25
    assert handler.opmon_conf == {
        "level": "INFO",
        "interval_s": 1.5,
        "path": "/tmp/ssh-opmon.json",
        "type": "file",
    }
    assert handler.opmon_uri == {"path": "/tmp/ssh-opmon-uri.json", "type": "file"}
    assert handler.log_path == "/tmp/logs"
    assert handler.opmon_publisher is publisher

    settings = handler.settings
    assert isinstance(settings, SSHProcessManagerSettings)
    assert settings.disable_localhost_host_key_check is True
    assert settings.disable_host_key_check is False
    assert settings.extra == {"discarded_ssh_setting": "unused"}
    assert settings.get("disable_localhost_host_key_check") is True
    assert settings.get("disable_host_key_check") is False
    assert settings.get("discarded_ssh_setting") == "unused"
    logger.warning.assert_called_once_with(
        "Discarding unsupported SSH process manager setting '%s'",
        "discarded_ssh_setting",
    )
    assert opmon_conf.application == "process_manager"
