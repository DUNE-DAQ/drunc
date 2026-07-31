"""
Store a dynamically generated list of configuration files by type for global access
throughout the application.
"""

from importlib import resources

# Define the path to the drunc data root directory
_DRUNC_DATA_ROOT = resources.files("drunc.data")

CONFIGURATION_TYPES = ["prrocess_mannager", "run_control"]

# Generate the set of process manager configurations
PROCESS_MANAGER_CONFIGS = [
    str(path)
    for path in (_DRUNC_DATA_ROOT / "process_manager").glob("process_manager/*.json")
]
# Strip out the ones that are no longer supported
PROCESS_MANAGER_CONFIGS.remove(
    "process-manager-k8s-pocket.json",
    "ssh-pocket-kafka.json",
    "ssh-standalone-paramiko-client.json",
)

# Generate the set of run control configurations

RUN_CONTROL_CONFIGS = [
    str(path) for path in (_DRUNC_DATA_ROOT / "run_control").glob("run_control/*.json")
]
