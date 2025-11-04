import pytest
import requests


def test_connectivity_service_started(
    one_controller_running: pytest.FixtureRequest,
) -> None:
    """
    Test to ensure that the connectivity service is started and reachable.

    Start the one controller session using the `one_controller_running` fixture, and
    send a GET request to the connectivity service endpoint to verify it is up and
    running.

    Args:
        one_controller_running: Pytest fixture that starts a DAQ session with one
            controller.

    Raises:
        requests.exceptions.RequestException: If the GET request fails.
        AssertionError: If the response status code is not 200.
    """
    processes_and_logs, session_dal, session_name = one_controller_running

    print("Attempting to setup a connection to the connectivity service")
    print(f"{session_dal.connectivity_service.service.port=}")
    print(f"{processes_and_logs=}")
    r = requests.get(
        f"http://localhost:{session_dal.connectivity_service.service.port}", timeout=5
    )
    print(f"Request: {r=}")

    r.raise_for_status()
    assert r.status_code == 200, "Connectivity service did not start in time"
