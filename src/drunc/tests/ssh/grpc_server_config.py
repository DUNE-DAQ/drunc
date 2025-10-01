from typing import Any, List, Tuple

from drunc.tests.ssh.available_grpc_servers import ServerType


class GrpcServerConfig:
    """
    Configuration container for a gRPC server instance.

    Encapsulates all parameters needed to start and manage a gRPC server,
    providing a consistent interface across different execution environments.
    """

    def __init__(
        self,
        server_id: str,
        server_type: ServerType,
        host: str,
        port: int,
        max_workers: int,
        log_file: str,
        server_options: List[Tuple[str, Any]] = None,
        client_options: List[Tuple[str, Any]] = None,
        **kwargs,
    ):
        """
        Initialise gRPC server configuration.

        Args:
            server_id: Unique identifier for this server instance
            server_type: Type of server (MANAGER, ROOT_CONTROLLER, or CHILD_CONTROLLER)
            host: Hostname or IP address where the server will run
            port: TCP port for the server to bind to
            max_workers: Maximum number of worker threads for the server
            log_file: Path to log file for server output
            server_options: gRPC server configuration options
            client_options: gRPC client configuration options (for servers that act as clients)
            **kwargs: Additional server-specific parameters (e.g., manager_port, root_port, child_name)
        """
        self.server_id = server_id
        self.server_type = server_type
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.log_file = log_file
        self.server_options = server_options or []
        self.client_options = client_options or []

        # Determine required parameters based on server type
        if server_type == ServerType.MANAGER:
            required_params = []
        elif server_type == ServerType.ROOT_CONTROLLER:
            required_params = ["manager_port"]
        elif server_type == ServerType.CHILD_CONTROLLER:
            required_params = ["root_port", "child_name"]
        else:
            required_params = []

        # Store extra parameters and validate required ones are present
        self.extra_params = {}
        for key, param in kwargs.items():
            if key in required_params:
                self.extra_params[key] = param
                required_params.remove(key)
            else:
                print(
                    f"Warning: Unrecognised parameter '{key}' for server type '{server_type}'"
                )

        # Check all required parameters were provided
        for param in required_params:
            raise ValueError(
                f"Missing required parameter '{param}' for server type '{server_type}'"
            )

    def get_param(self, key: str, default: Any = None) -> Any:
        """
        Get an additional parameter value.

        Args:
            key: Parameter name to retrieve
            default: Default value if parameter not found

        Returns:
            Parameter value or default if not found
        """
        return self.extra_params.get(key, default)
