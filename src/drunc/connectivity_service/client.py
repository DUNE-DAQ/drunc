import time

from requests.exceptions import ConnectionError, HTTPError, ReadTimeout

from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.utils.utils import get, get_logger, http_post


class ConnectivityServiceClient:
    def __init__(self, session: str, address: str):
        self.session = session
        self.log = get_logger("utils.ConnectivityServiceClient")

        if address.startswith("http://") or address.startswith("https://"):
            self.address = address
        else:
            # assume the simplest case here
            self.address = f"http://{address}"

        self.log.debug(
            f"Connectivity service address: {self.address}, session: {self.session}"
        )

    def is_ready(self, timeout: int = 10):
        """
        Check if the service is ready by polling the connectivity service
        with an exponential backoff.

        Args:
            timeout: Maximum time in seconds to wait for the service to become ready

        Returns:
            True if service becomes ready within timeout, False otherwise
        """
        start = time.time()
        attempt = 0
        delay = 0.1  # Initial delay in seconds
        max_delay = 2.0  # Maximum delay between attempts

        while time.time() - start < timeout:
            attempt += 1
            elapsed = time.time() - start

            self.log.info(f"Health check attempt {attempt} at {elapsed:.2f}s elapsed")
            self.log.info(f"Polling address: {self.address}")
            try:
                r = get(self.address)
            except Exception as e:
                self.log.info(f"Polling failed with exception: {e}")
                r = None
            # Request failed - service is NOT ready.
            if r is None or not r.ok:
                self.log.info(
                    f"Connectivity service not ready, retrying in {delay:.2f}s"
                )
                time.sleep(delay)
                # Increase delay for next time.
                delay = min(delay * 2, max_delay)
            # Request succeeded - service is ready.
            else:
                self.log.info(
                    f"Connectivity service ready after {attempt} attempts ({elapsed:.2f}s)"
                )
                return True

        # 04-Feb-2026, KAB: increased the severity of the 'Connectivity service not ready' message
        # so that users are notified that this happened.
        self.log.error(
            f"Connectivity service not ready after {attempt} attempts. Timeout of {timeout}s reached."
        )
        return False

    def retract(self, uid, fail_quickly=False):
        data = {
            "partition": self.session,
            "connections": [
                {
                    "connection_id": uid,
                    "data_type": "RunControlMessage",
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
                    self.log.debug(
                        f"Connection '{uid}' not found on the connectivity service"
                    )
                    break

                r.raise_for_status()
                break

            except (HTTPError, ConnectionError, ReadTimeout) as e:
                self.log.debug(e)
                if not fail_quickly:
                    time.sleep(0.5)

            except Exception as e:
                if fail_quickly:
                    self.log.info(
                        f"Could not retract {uid} from session {self.session} on the connectivity service at the address {self.address}"
                    )
                    self.log.debug(e)
                else:
                    raise e

            finally:
                if fail_quickly:
                    return

    def retract_partition(
        self, fail_quickly: bool = False, fail_quietly: bool = False
    ) -> None:
        """
        Retract the whole partition (session) from the connectivity service.

        Args:
            fail_quickly (bool): If True, the function will return immediately on failure without
                                 raising exceptions. Default is False.
            fail_quietly (bool): If True, the function will suppress all exceptions and log
                                 errors as warnings. Default is False.
        """
        data = {"partition": self.session}
        for i in range(50):
            try:
                self.log.debug(
                    f"Retracting session {self.session} on the connectivity service, attempt {i + 1}: {data=}"
                )

                r = http_post(
                    self.address + "/retract-partition",
                    data=data,
                    headers={"Content-Type": "application/json"},
                    as_json=True,
                    timeout=0.5,
                    ignore_errors=True,
                )

                if r.status_code == 404:
                    if not fail_quietly:
                        self.log.warning(
                            f"Session {self.session} not found on the connectivity service"
                        )
                    break

                r.raise_for_status()
                break

            except (HTTPError, ConnectionError, ReadTimeout) as e:
                self.log.debug(e)
                if not fail_quickly:
                    time.sleep(0.5)

            except Exception as e:
                if fail_quickly:
                    if not fail_quietly:
                        self.log.info(
                            f"Could not retract session {self.session} on the connectivity service at the address {self.address}"
                        )
                        self.log.debug(e)
                else:
                    raise e

            finally:
                if fail_quickly:
                    return

    def resolve(self, uid_regex: str, data_type: str, ntries=50) -> dict:
        data = {"data_type": data_type, "uid_regex": uid_regex}
        for i in range(ntries):
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
