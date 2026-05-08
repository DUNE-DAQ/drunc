import requests

from drunc.utils.utils import get_logger


class ResourceManagerClient:
    """
    Interface for communicating with the Resource Manager service.
    """

    def __init__(self, base_url):
        """
        Initialize the ResourceManagerClient with the base URL of the Resource Manager service.
        """
        self.url = base_url.rstrip("/")
        self.log = get_logger("resource_manager.client")

    def _send_request(self, endpoint, payload):
        """
        Helper method to send a POST request to the Resource Manager and handle responses.

        Args:
            endpoint (str): The full URL endpoint to send the request to
            payload (dict): The data payload to send in the request

        Returns:
            dict: The JSON response from the server if successful, or None if an error occurred

        Raises:
            None: Logs errors and returns None instead of raising exceptions for HTTP errors or unexpected issues.
        """
        try:
            # verify=False is used here for local/self-signed certs (like curl -k)
            self.log.debug(f"Sending request to {endpoint} with payload: {payload}")
            response = requests.post(endpoint, data=payload, verify=False)

            if "application/json" not in response.headers.get("Content-Type", ""):
                self.log.error(
                    "Server returned HTML/Text instead of JSON. Check your URL paths."
                )
                return None

            # Raise an exception for 4xx or 5xx status codes
            response.raise_for_status()

            # Log the successful response
            return response.json()

        except requests.exceptions.HTTPError:
            self.log.warning(f"Request failed with status {response.status_code}")
            self.log.debug(f"Response content: {response.text}")
            return response.json()
        except Exception as e:
            self.log.error(f"An unexpected error occurred: {e}")
            return None

    def query_resources(
        self, resources: list[str], owner: str, session_id: str, session_name: str
    ):
        """
        Query the Resource Manager for the status of the specified resources.

        Args:
            resources (list[str]): List of resource names to query
            owner (str): The new owner of the resources
            session_id (str): The session ID taking the resources
            session_name (str): The session name taking the resources

        Returns:
            dict: A dictionary containing the status of the queried resources
        """
        payload = {
            "names": ",".join(resources),
        }
        endpoint = f"{self.url}/api/query_resource/"

        return self._send_request(endpoint, payload)

    def request_resources(
        self, resources: list[str], owner: str, session_id: str, session_name: str
    ):
        """
        Request resources from the Resource Manager for isolation during the run.

        Args:
            resources (list[str]): List of resource names to query
            owner (str): The new owner of the resources
            session_id (str): The session ID taking the resources
            session_name (str): The session name taking the resources

        Returns:
            dict: A dictionary containing the status of the queried resources
        """
        payload = {
            "names": ",".join(resources),
            "user_name": owner,
            "session_id": session_id,
            "session_name": session_name,
        }
        endpoint = f"{self.url}/api/request_resource/"

        return self._send_request(endpoint, payload)

    def release_resources(self, resources: list[str], session_id: str):
        """
        Release resources from the Resource Manager for other runs to use.

        Args:
            resources (list[str]): List of resource names to query
            session_id (str): The session ID taking the resources

        Returns:
            dict: A dictionary containing the status of the queried resources
        """
        payload = {
            "names": ",".join(resources),
            "session_id": session_id,
        }
        endpoint = f"{self.url}/api/release_resource/"

        return self._send_request(endpoint, payload)
