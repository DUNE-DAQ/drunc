"""
Store a dynamically generated list of configuration files by type for global access
throughout the application.
"""

from importlib import resources

# Define the path to the drunc data root directory
_DRUNC_DATA_ROOT = resources.files("drunc.data")

CONFIGURATION_TYPES = ["process_manager", "run_control"]

# Generate the set of process manager configurations
PROCESS_MANAGER_CONFIGS = [
    str(path.name) for path in (_DRUNC_DATA_ROOT / "process_manager").glob("*.json")
]

# Strip out the ones that are no longer supported
UNSUPPORTED_PROCESS_MANAGERS = (
    "process-manager-k8s-pocket.json",
    "ssh-pocket-kafka.json",
    "ssh-standalone-paramiko-client.json",
)
PROCESS_MANAGER_CONFIGS = [
    config
    for config in PROCESS_MANAGER_CONFIGS
    if not any(unsupported in config for unsupported in UNSUPPORTED_PROCESS_MANAGERS)
]
# Generate the set of run control configurations

RUN_CONTROL_CONFIGS = [
    str(path) for path in (_DRUNC_DATA_ROOT / "run_control").glob("run_control/*.json")
]
