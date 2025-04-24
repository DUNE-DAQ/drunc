import requests


def test_connectivity_service_started(one_controller_running):
    session_dal = one_controller_running[1]

    r = requests.get(
        f"http://localhost:{session_dal.connectivity_service.service.port}", timeout=2
    )

    r.raise_for_status()
    assert r.status_code == 200, "Connectivity service did not start in time"
