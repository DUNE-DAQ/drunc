from enum import Enum


class ServerType(Enum):
    """Enumeration of supported server types with their corresponding CLI scripts."""

    MANAGER = "process_manager_server_cli.py"
    ROOT_CONTROLLER = "root_controller_server_cli.py"
    CHILD_CONTROLLER = "child_controller_server_cli.py"
