from unittest.mock import Mock

import pytest

from drunc.process_manager import k8s_process_manager_settings
from drunc.process_manager.configuration import ProcessManagerTypes
from drunc.process_manager.k8s_process_manager_settings import K8sProcessManagerSettings
from tests.process_manager.conftest import ProcessManagerFromJson


def test_process_manager_from_json_loads_k8s_settings(
    process_manager_from_json: ProcessManagerFromJson,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = Mock()
    monkeypatch.setattr(
        k8s_process_manager_settings, "get_logger", Mock(return_value=logger)
    )
    config_data: dict[str, object] = {
        "type": "k8s",
        "environment": {"RUN_NUMBER": 42, "PARTITION": "np04"},
        "settings": {
            "labels": {"app": "readout", "run": 42},
            "readout_app_selector": "readout",
            "service": {"headless_discovery_port": 31000},
            "pod_management": {"kill_timeout": 10.5, "pod_ready_timeout": 45},
            "cleanup": {
                "restart_cleanup_time": 8,
                "restart_cleanup_polling": 0.5,
                "kill_watch_fallback_poll_interval": 2,
            },
            "volumes": [
                {
                    "name": "data",
                    "mount_path": "/data",
                    "host_path": "/srv/data",
                    "read_only": True,
                }
            ],
            "home_path_base": "/home/drunc",
            "checking": {
                "watcher_retry_sleep": 1,
                "pod_status_check_sleep": 2,
                "host_cache_expiry": 3,
                "service_startup_timeout": 4,
                "socket_retry_timeout": 5.5,
            },
            "host_configs": {
                "host-a": {
                    "limits": {"cpu": 2, "memory": "4Gi"},
                    "requests": {"cpu": "1", "memory": "2Gi"},
                }
            },
            "discarded_k8s_setting": "unused",
        },
        "opmon_conf": {
            "level": "DEBUG",
            "interval_s": 2,
            "path": "/tmp/k8s-opmon.json",
            "type": "file",
        },
        "opmon_uri": {"path": "/tmp/k8s-opmon-uri.json", "type": "file"},
        "image": "ghcr.io/dune-daq/test-image:latest",
    }

    handler, logger, opmon_conf, publisher = process_manager_from_json(
        config_data, logger
    )

    assert handler.initial_data.type == "k8s"
    assert handler.initial_data.environment == {"RUN_NUMBER": "42", "PARTITION": "np04"}
    assert handler.environment == {"RUN_NUMBER": "42", "PARTITION": "np04"}
    assert handler.pm_type is ProcessManagerTypes.K8s
    assert handler.image == "ghcr.io/dune-daq/test-image:latest"
    assert handler.opmon_conf == {
        "level": "DEBUG",
        "interval_s": 2,
        "path": "/tmp/k8s-opmon.json",
        "type": "file",
    }
    assert handler.opmon_uri == {"path": "/tmp/k8s-opmon-uri.json", "type": "file"}
    assert handler.log_path == "/tmp/logs"
    assert handler.opmon_publisher is publisher

    settings = handler.settings
    assert isinstance(settings, K8sProcessManagerSettings)
    assert settings.labels == {"app": "readout", "run": "42"}
    assert settings.readout_app_selector == "readout"
    assert settings.service == {"headless_discovery_port": 31000}
    assert settings.pod_management == {"kill_timeout": 10.5, "pod_ready_timeout": 45}
    assert settings.cleanup == {
        "restart_cleanup_time": 8,
        "restart_cleanup_polling": 0.5,
        "kill_watch_fallback_poll_interval": 2,
    }
    assert settings.volumes == [
        {
            "name": "data",
            "mount_path": "/data",
            "host_path": "/srv/data",
            "read_only": True,
        }
    ]
    assert settings.home_path_base == "/home/drunc"
    assert settings.checking == {
        "watcher_retry_sleep": 1,
        "pod_status_check_sleep": 2,
        "host_cache_expiry": 3,
        "service_startup_timeout": 4,
        "socket_retry_timeout": 5.5,
    }
    assert settings.host_configs == {
        "host-a": {
            "limits": {"cpu": "2", "memory": "4Gi"},
            "requests": {"cpu": "1", "memory": "2Gi"},
        }
    }
    assert settings.extra == {"discarded_k8s_setting": "unused"}
    assert settings.get("labels") == {"app": "readout", "run": "42"}
    assert settings.get("discarded_k8s_setting") == "unused"
    logger.warning.assert_called_once_with(
        "Discarding unsupported K8s process manager setting '%s'",
        "discarded_k8s_setting",
    )
    assert opmon_conf.application == "process_manager"
