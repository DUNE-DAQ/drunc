import os
import time

import pytest

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.exceptions import DruncException


def test_controller_init(one_controller_running):
    controller_process = one_controller_running[0]["controller-0"]
    time_inc = 0
    timeout = 4
    found = False

    while time_inc < timeout:
        if os.path.exists(controller_process[1]):
            with open(controller_process[1], "r") as f:
                for line in f.readlines():
                    if "Controller ready" in line:
                        found = True
                        break
        if found:
            break
        time.sleep(0.1)
        time_inc += 0.1

    assert timeout > time_inc, "Controller did not start in time"


def test_controller_registered_on_connectivity_service(one_controller_running):
    session_dal = one_controller_running[1]
    connectivity_service_port = session_dal.connectivity_service.service.port
    session_name = one_controller_running[2]
    con_server_process, con_server_log_file = one_controller_running[0][
        "local-connection-server"
    ]

    csc = ConnectivityServiceClient(
        session_name,
        f"localhost:{connectivity_service_port}",
    )

    controller_address = None
    try:
        controller_address = csc.resolve(
            "controller-0_control", "RunControlMessage", ntries=20
        )
    except DruncException as e:
        pytest.fail(f"Controller did not advertise its address in time: {e!s}")

    assert len(controller_address) == 1
    assert controller_address[0]["uri"].startswith("grpc://")
