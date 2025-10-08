from enum import Enum


class ServerType(Enum):
    """Enumeration of supported server types with their corresponding CLI scripts."""

    MANAGER = 0
    ROOT_CONTROLLER = 1
    CHILD_CONTROLLER = 2
