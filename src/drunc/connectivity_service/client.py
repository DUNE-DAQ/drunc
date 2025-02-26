import time

from requests.exceptions import ConnectionError, HTTPError, ReadTimeout

from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.utils.utils import get_logger, http_post


class ConnectivityServiceClient:
    def __init__(self, session: str, address: str):
        self.session = session
        self.log = get_logger("utils.ConnectivityServiceClient")

        if address.startswith("http://") or address.startswith("https://"):
            self.address = address
        else:
            # assume the simplest case here
            self.address = f"http://{address}"

    def retract(self, uid):
        data = {
            "partition": self.session,
            "connections": [
                {
                    "connection_id": uid,
                    "data_type": "run-control-messages",
                }
            ],
        }
        for i in range(50):
            try:
                self.log.debug(
                    f"Retracting '{uid}' on the connectivity service, attempt {i + 1}"
                )

                r = http_post(
                    self.address + "/retract",
                    data=data,
                    headers={"Content-Type": "application/json"},
                    as_json=True,
                    timeout=0.5,
                    ignore_errors=True,
                )

                if r.status_code == 404:
                    self.log.warning(
                        f"Connection '{uid}' not found on the application registry"
                    )
                    break

                r.raise_for_status()
                break
            except (HTTPError, ConnectionError):
                time.sleep(0.5)
                continue

    def resolve(self, uid_regex: str, data_type: str) -> dict:
        data = {"data_type": data_type, "uid_regex": uid_regex}
        for i in range(50):
            try:
                self.log.debug(
                    f"Looking up '{uid_regex}' on the connectivity service, attempt {i + 1}"
                )
                response = http_post(
                    self.address + "/getconnection/" + self.session,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    as_json=True,
                    timeout=0.5,
                    ignore_errors=True,
                )
                response.raise_for_status()
                content = response.json()
                if content:
                    return content
                else:
                    self.log.debug(
                        f"Could not find the address of '{uid_regex}' on the application registry"
                    )
                    time.sleep(0.2)

            except (HTTPError, ConnectionError, ReadTimeout) as e:
                self.log.debug(e)
                time.sleep(0.2)
                continue

        self.log.debug(
            f"Could not find the address of '{uid_regex}' on the application registry"
        )
        raise ApplicationLookupUnsuccessful

    def publish(self, uid, uri, data_type: str):
        for i in range(50):
            try:
                self.log.debug(
                    f"Publishing '{uid}' on the connectivity service, attempt {i + 1}"
                )

                http_post(
                    self.address + "/publish",
                    data={
                        "partition": self.session,
                        "connections": [
                            {
                                "connection_type": 0,
                                "data_type": data_type,
                                "uid": uid,
                                "uri": uri,
                            }
                        ],
                    },
                    headers={"Content-Type": "application/json"},
                    as_json=True,
                    timeout=0.5,
                    ignore_errors=True,
                ).raise_for_status()
                break
            except (HTTPError, ConnectionError, ReadTimeout):
                time.sleep(0.2)
                continue
