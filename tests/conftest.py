import getpass
import logging
import os
import time
from pathlib import Path
from subprocess import Popen

import psutil
import pytest

logger = logging.getLogger(__name__)

consolidated_conf_path = f"/tmp/drunc-pytests-of-{getpass.getuser()}"


@pytest.fixture
def load_test_config() -> None:
    """
    Fixture to set up the test configuration environment.

    It sets the DUNEDAQ_DB_PATH environment variable to include paths to test
    configurations and a consolidated configuration directory.

    This fixture is automatically used by other fixtures and tests that require
    access to the test configurations.
    """

    # Set up the DUNEDAQ_DB_PATH environment variable
    DUNEDAQ_DB_PATH: str | None = os.getenv("DUNEDAQ_DB_PATH")
    if DUNEDAQ_DB_PATH is None:
        DUNEDAQ_DB_PATH = ""

    # Determine the path to the test configurations
    cwd = Path(os.path.abspath(__file__))
    test_configs = cwd.parent / ".." / "config" / "tests"
    test_configs = test_configs.resolve()
    print(f"{test_configs=}")

    # Ensure the consolidated configuration directory exists
    os.makedirs(consolidated_conf_path, exist_ok=True)
    DUNEDAQ_DB_PATH += f":{test_configs!s}:{consolidated_conf_path!s}"

    # For debugging, print the DUNEDAQ_DB_PATH entries
    print("DUNEDAQ_DB_PATH entries:")
    for entry in DUNEDAQ_DB_PATH.split(":"):
        print(f"\t{entry}")
    print("")

    # Set the environment variable
    os.environ["DUNEDAQ_DB_PATH"] = DUNEDAQ_DB_PATH


def boot_session(
    configuration_name: str, request: pytest.FixtureRequest
) -> tuple[dict, object, str]:
    """
    Boots a DAQ session based on the provided configuration name.

    It consolidates the configuration, sets up the environment, and starts
    the necessary processes.

    Args:
        configuration_name (str): The name of the configuration to use.
        request (pytest.FixtureRequest): The pytest request object for accessing test
            metadata.

    Returns:
        tuple: A tuple containing:
            - dict: A dictionary of process names to their Popen objects and log file
                paths.
            - object: The session DAL object.
            - str: The session name.

    Raises:
        ImportError: If the 'conffwk' module is not installed.
    """

    # Set up the environment and consolidate the configuration
    from daqconf.consolidate import consolidate_db

    from drunc.process_manager.oks_parser import collect_apps, collect_infra_apps

    req_name: str = request.node.name
    configuration_file: str = f"{configuration_name}.data.xml"
    configuration_consolidated_file: str = (
        f"{consolidated_conf_path}/{configuration_name}.{req_name}"
        ".consolidated.data.xml"
    )
    consolidate_db(configuration_file, configuration_consolidated_file)

    # Set the connectivity service port
    from daqconf.set_connectivity_service_port import set_connectivity_service_port

    set_connectivity_service_port(configuration_consolidated_file, configuration_name)
    session_name: str = f"{req_name}-{configuration_name}"

    # Load the configuration and start the processes
    try:
        import conffwk
    except ImportError:
        pytest.skip("conffwk is not installed")

    # Prepare environment variables
    env = os.environ.copy()
    env["DUNEDAQ_SESSION"] = session_name

    # Load the configuration, get the DAL
    configuration_consolidated_file = f"oksconflibs:{configuration_consolidated_file}"
    db = conffwk.Configuration(configuration_consolidated_file)
    session_dal = db.get_dal(class_name="Session", uid=configuration_name)

    # Collect applications to run
    apps = collect_apps(
        session_name=session_name,
        config_filename=configuration_consolidated_file,
        session_obj=session_dal,
        segment_obj=session_dal.segment,
        env=env,
        tree_prefix=[0],
    )
    next_tree_id = max([int(app["tree_id"].split(".")[0]) for app in apps]) + 1
    apps += collect_infra_apps(session=session_dal, env=env, tree_prefix=[next_tree_id])

    # Start the processes
    processes: dict[str, tuple[Popen, str]] = {}
    for app_info in apps:
        log_file = (
            "log_"
            + getpass.getuser()
            + "_"
            + session_name
            + "_"
            + app_info["name"]
            + ".txt"
        )
        log_file = consolidated_conf_path + "/" + log_file
        args = f"{app_info['type']} {' '.join(app_info['args'])} > {log_file}  | sed -u 's/\\x1b\\[[0-9;]*m//g' 2>&1"
        print(f"{args=}")
        logger.debug(f"Running {args}")
        # Use the parent process's session to avoid gunicorn detecting a parent change.
        # Set start_new_session=False (default) and do not daemonize.
        process = Popen(
            args=args,
            env=app_info["env"],
            shell=True,
            start_new_session=True,
        )

        processes[app_info["name"]] = process, log_file

    print(f"Started processes: {processes}")

    for _ in range(10):
        if os.path.exists(processes["local-connection-server"][1]):
            with open(processes["local-connection-server"][1], "r") as f:
                if "[INFO] Starting gunicorn" in f.readline():
                    print(f"Gunicorn has started for {session_name}")
                    break
        time.sleep(0.1)
        print(f"Waiting for gunicorn to start, iteration {_}")

    return processes, session_dal, session_name


def cleanup(processes: dict[str, tuple[Popen, str]]) -> None:
    """
    Clean up the processes and their log files.

    This function iterates over the provided processes, kills any that are still
    running, and removes their associated log files.

    Args:
        processes (dict): A dictionary of process names to their Popen objects and log
            file paths.

    Returns: None
    """

    for _, process_and_log in processes.items():
        proc = process_and_log[0]
        if proc.poll() is None:
            proc.kill()


@pytest.fixture(scope="function")
def one_controller_running(
    load_test_config: pytest.FixtureRequest, request: pytest.FixtureRequest
):
    """
    Fixture to boot a DAQ session with one controller running.

    It uses the 'one-controller-config' configuration and ensures that the session is
    properly cleaned up after the test.

    Args:
        load_test_config: Pytest fixture to set up test configuration.
        request: Pytest request object.

    Yields:
        tuple: (processes_and_logs, session_dal, session_name)
    """

    print("Starting one_controller_running processes")
    configuration_name = "one-controller-config"
    processes_and_logs, session_dal, session_name = boot_session(
        configuration_name, request
    )
    time.sleep(2)  # Give gunicorn time to start
    yield processes_and_logs, session_dal, session_name

    print("Cleaning up one_controller_running processes")
    cleanup(processes_and_logs)


@pytest.fixture
def many_controllers_running(load_test_config, request):
    configuration_name = "deep-segments-config"
    processes_and_logs, session_dal, session_name = boot_session(
        configuration_name, request
    )
    yield processes_and_logs, session_dal, session_name
    cleanup(processes_and_logs)


def pytest_sessionfinish(session, exitstatus):
    """
    Pytest hook containing the tear-down steps to kill running processes
    and remove log files.
    """
    import glob

    # Iterate over all running processes
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        cmdline = proc.info.get("cmdline")
        if isinstance(cmdline, list):
            joined = " ".join(cmdline)
            if "gunicorn" in joined or "drunc-controller" in joined:
                try:
                    proc.kill()
                except Exception as e:
                    print(f"Failed to kill process {proc.pid}: {e}")

    # Remove logs
    for path in glob.glob("info.test*"):
        try:
            os.remove(path)
        except Exception as e:
            print(f"Failed to delete {path}: {e}")
