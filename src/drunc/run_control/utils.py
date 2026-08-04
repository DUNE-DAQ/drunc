from enum import Enum
from urllib.parse import urlparse

from drunc.configuration_files import PROCESS_MANAGER_CONFIGS


class ProcessManagerDeploymentType(Enum):
    """
    Enum for the process manager deployment type.
    """

    UNKNOWN = 0
    INTERNAL = 1
    EXTERNAL = 2


def determine_process_manager_type(
    process_manager: str,
) -> ProcessManagerDeploymentType:
    """
    Check if the process manager string is a configuration file or a URI.

    When deploying the run control, we need to determine whether the process manager
    is a configuration file or a URI. This function checks the process manager string
    and returns the type

    Args:
        process_manager (str): The name of the process manager to check.

    Returns:
        ProcessManagerDeploymentType: The type of the process manager deployment.

    """
    urlparse_result = urlparse(process_manager)
    if urlparse_result.scheme:
        return ProcessManagerDeploymentType.EXTERNAL
    elif urlparse_result.path in PROCESS_MANAGER_CONFIGS:
        return ProcessManagerDeploymentType.INTERNAL
    else:
        return ProcessManagerDeploymentType.UNKNOWN
