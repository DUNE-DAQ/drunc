import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from drunc.process_manager.k8s_process_manager import (
    K8sPodWatcherThread,
    K8sProcessManager,
)


def test_watcher_processes_restarted_pod_with_same_process_uuid(monkeypatch):
    pods = [
        SimpleNamespace(
            metadata=SimpleNamespace(
                labels={"uuid.drunc.daq": "process-uuid"},
                uid=pod_uid,
                namespace="session",
            ),
            status=SimpleNamespace(phase="Failed", container_statuses=[]),
        )
        for pod_uid in ("old-pod-uid", "new-pod-uid")
    ]

    class StopWatching(BaseException):
        pass

    class FakeWatch:
        def stream(self, *_args, **_kwargs):
            for pod in pods:
                yield {"object": pod, "type": "DELETED"}
            raise StopWatching

    pm = Mock()
    pm.drunc_label = "drunc.daq"
    monkeypatch.setattr(
        "drunc.process_manager.k8s_process_manager.watch.Watch", FakeWatch
    )

    with pytest.raises(StopWatching):
        K8sPodWatcherThread(pm).run()

    assert [call.args[:2] for call in pm.notify_termination.call_args_list] == [
        ("process-uuid", "old-pod-uid"),
        ("process-uuid", "new-pod-uid"),
    ]


def test_termination_of_restarted_pod_completes_its_own_batch():
    pm = K8sProcessManager.__new__(K8sProcessManager)
    pm._pending_deletion_lock = threading.Lock()
    pm._pending_deletion_batches = {}
    pm._notified_pods = set()
    pm.boot_request = {}
    pm.log = Mock()

    pm.notify_termination("process-uuid", "old-pod-uid", -1, "Deleted", "session")

    remaining_pod_uids = {"new-pod-uid"}
    batch_event = threading.Event()
    pm._pending_deletion_batches["new-pod-uid"] = (remaining_pod_uids, batch_event)
    pm.notify_termination("process-uuid", "new-pod-uid", -1, "Deleted", "session")

    assert batch_event.is_set()
    assert not remaining_pod_uids
