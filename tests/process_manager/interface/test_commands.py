"""
Tests for CLI commands using Click's built-in testing utility CliRunner.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from drunc.process_manager.interface.commands import (InterruptedCommand, boot,
                                                      dummy_boot, flush, kill,
                                                      logs, ps, restart,
                                                      terminate)


@pytest.fixture
def boot_arguments():
    """
    Fixture containing the arguments used in `boot`.
    """
    return [
        "--user",
        "testuser",
        "--no-override-logs",
        "dummy_config.yaml",
        "test-session",
        "conf-id-123",
    ]


@pytest.fixture
def dummy_boot_arguments():
    """
    Fixture containing the arguments used in `dummy_boot`.
    """
    return [
        "--user",
        "testuser",
        "--n-processes",
        "2",
        "--sleep",
        "5",
        "--n_sleeps",
        "3",
        "test_session",  # the positional argument
    ]


@pytest.fixture
def mock_logger():
    with patch("drunc.process_manager.interface.commands.get_logger") as get_logger:
        logger = MagicMock()
        get_logger.return_value = logger
        yield logger


@pytest.fixture
def mock_tabulate():
    with patch(
        "drunc.process_manager.interface.commands.tabulate_process_instance_list"
    ) as tabulate:
        tabulate.return_value = "Formatted output"
        yield tabulate


class MockDriver:
    """
    Simulate a real process manager driver object with the methods that are not mocked inside the tests.
    """

    def __init__(
        self, existing_processes=None, boot_result=None, terminate_result=None
    ):
        self._existing_processes = existing_processes or []
        self._boot_result = boot_result or [
            MockBootResult([("proc1", "uuid-123")]),
            MockBootResult([("process2", "uuid-456")]),
        ]
        self._terminate_result = terminate_result or self._boot_result
        self.controller_address = "localhost:5000"

    def ps(self, query=None):
        self._ps_called_with = query
        return MagicMock(values=self._existing_processes)

    def boot(self, **kwargs):
        return self._boot_result

    def dummy_boot(self, **kwargs):
        return self._boot_result

    def logs(self, log_request):
        mock_result = MagicMock()
        mock_result.uuid.uuid = "uuid-123"
        mock_result.lines = []
        return mock_result


class MockContext:
    """
    Simulate the CLI context object passed to commands.
    """

    def __init__(self, driver=None):
        self.driver = driver or MockDriver()
        self.output = []

    def get_driver(self, name):
        return self.driver

    def print(self, msg, justify=None):
        self.output.append(str(msg))


class MockBootResult:
    """
    Simulate a boot call result containing one or more process instances.
    """

    def __init__(self, processes):
        self.values = []
        for name, uuid in processes:
            process = MagicMock()
            process.process_description = MagicMock()
            process.process_description.metadata = MagicMock()
            process.process_description.metadata.name = name
            process.uuid = MagicMock()
            process.uuid.uuid = uuid
            self.values.append(process)


def test_terminate_command(mock_logger, mock_tabulate):
    """
    Test that the terminate command logs correctly and prints formatted output.
    """

    mock_driver = MagicMock()
    mock_driver.terminate.return_value = [{"name": "process1", "uuid": 1234}]

    mock_context = MockContext(driver=mock_driver)

    mock_context.get_driver = MagicMock(return_value=mock_driver)
    mock_context.delete_driver = MagicMock()

    result = CliRunner().invoke(terminate, obj=mock_context)

    assert result.exit_code == 0  # command completed succesfully

    mock_logger.debug.assert_called_with("Terminating")
    mock_tabulate.assert_called_once()
    mock_context.get_driver.assert_called_with("process_manager")
    mock_context.delete_driver.assert_called_with("controller")


def test_terminate_no_processes(mock_logger):
    """
    Test that the terminate command handles no processes to terminate.
    """

    mock_driver = MagicMock()
    mock_driver.terminate.return_value = ""

    mock_context = MockContext(driver=mock_driver)
    mock_context.delete_driver = MagicMock()

    result = CliRunner().invoke(terminate, obj=mock_context)

    assert result.exit_code == 0

    mock_logger.debug.assert_called_with("Terminating")

    assert mock_context.output == []
    mock_context.delete_driver.assert_not_called()


def test_boot_command_successful(mock_logger, boot_arguments):
    """
    Test a successful boot command with no existing processes.
    """

    mock_driver = MockDriver(
        existing_processes=[],
        boot_result=[
            MockBootResult([("process1", "uuid-123")]),
            MockBootResult([("process2", "uuid-456")]),
        ],
        terminate_result=[],
    )

    mock_context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(boot, boot_arguments, obj=mock_context)

    assert result.exit_code == 0  # command ran succesfully

    # check that each process was logged as started
    mock_logger.debug.assert_any_call("'process1' (uuid-123) process started")
    mock_logger.debug.assert_any_call("'process2' (uuid-456) process started")


def test_boot_exiting_processes_abort(boot_arguments):
    """
    Test user aborts command when existing processes.
    """

    existing_process = [MagicMock(), MagicMock()]
    mock_driver = MockDriver(existing_processes=existing_process, boot_result=[])

    mock_driver.boot = MagicMock()  # to check if boot is called

    mock_context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(
        boot,
        boot_arguments,
        obj=mock_context,
        input="n\n",  # simulate user typing 'n' to abort
    )

    assert result.exit_code != 0  # boot command exits with error
    assert isinstance(result.exception, SystemExit)  # system exits on abort

    # check that 'boot' was never called
    mock_driver.boot.assert_not_called()

    assert (
        "You already have 2 processes running, are you sure you want to boot a session?"
        in result.output
    )


def test_boot_exiting_processes_user_confirm(boot_arguments):
    """
    Test when user confirms 'boot' command when there are existing processes.
    """

    existing_process = [MagicMock(), MagicMock()]
    mock_driver = MockDriver(existing_processes=existing_process, boot_result=[])

    mock_driver.boot = MagicMock()

    mock_context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(
        boot,
        boot_arguments,
        obj=mock_context,
        input="y",  # simulate user typing 'y' to confirm
    )

    assert result.exit_code == 0

    mock_driver.boot.assert_called()

    assert (
        "You already have 2 processes running, are you sure you want to boot a session?"
        in result.output
    )


def test_boot_interrupted_command(boot_arguments):
    """
    Test that boot exits gracefully when InterruptedCommand is raised.
    """

    mock_driver = MockDriver(existing_processes=[])

    mock_driver.boot = MagicMock(side_effect=InterruptedCommand())
    mock_context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(boot, boot_arguments, obj=mock_context)

    assert result.exit_code == 0
    mock_driver.boot.assert_called_once()


def test_boot_missing_controller_address(mock_logger, boot_arguments):
    """
    Test boot command when the root controller address is missing.
    """

    mock_driver = MockDriver()
    mock_driver.controller_address = None
    context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(boot, boot_arguments, obj=context)

    assert result.exit_code == 0
    mock_logger.error.assert_called_once()
    controller_missing_msg = (
        "Could not understand where the controller is! "
        "You can look at the logs of the controller to see its address"
    )
    assert controller_missing_msg in mock_logger.error.call_args[0][0]


def test_dummy_boot_command_successful(mock_logger, dummy_boot_arguments):
    """
    Test a successful dummy_boot command with no existing processes.
    """

    mock_driver = MockDriver(
        existing_processes=[],
        boot_result=[
            MockBootResult([("process1", "uuid-123")]),
            MockBootResult([("process2", "uuid-456")]),
        ],
        terminate_result=[],
    )

    mock_context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(dummy_boot, dummy_boot_arguments, obj=mock_context)

    assert result.exit_code == 0

    # Check that each process was logged as started
    mock_logger.debug.assert_any_call("'process1' (uuid-123) process started")
    mock_logger.debug.assert_any_call("'process2' (uuid-456) process started")


def test_dummy_boot_command_interrupted(mock_logger, dummy_boot_arguments):
    """
    Test that the dummy_boot command handles interruptions gracefully.
    """

    mock_driver = MockDriver(existing_processes=[])

    mock_driver.dummy_boot = MagicMock(side_effect=InterruptedCommand())

    mock_context = MockContext(driver=mock_driver)

    result = CliRunner().invoke(dummy_boot, dummy_boot_arguments, obj=mock_context)

    assert result.exit_code == 0
    mock_driver.dummy_boot.assert_called_once()


def test_kill_command(mock_tabulate):
    """
    Test the kill command.
    """

    mock_driver = MockDriver()
    mock_driver.kill = MagicMock()
    mock_context = MockContext(driver=mock_driver)
    mock_context.delete_driver = MagicMock()

    dummy_kill_arguments = ["--name", "process1"]
    result = CliRunner().invoke(kill, dummy_kill_arguments, obj=mock_context)

    assert result.output == ""
    assert result.exit_code == 0
    mock_driver.kill.assert_called_once()


def test_flush(mock_tabulate):
    """
    Test the flush command.
    """

    mock_driver = MockDriver()
    mock_driver.flush = MagicMock()
    mock_context = MockContext(driver=mock_driver)
    mock_context.print = MagicMock()
    mock_context.rule = MagicMock()

    dummy_flush_arguments = ["--name", "process1"]
    result = CliRunner().invoke(flush, dummy_flush_arguments, obj=mock_context)

    assert result.exit_code == 0
    mock_driver.flush.assert_called_once()


def test_logs_command_with_grep_and_lines():
    """
    Test the logs command with grep and lines options.
    """

    mock_driver = MockDriver()
    mock_context = MockContext(driver=mock_driver)
    mock_context.rule = MagicMock()
    mock_context.print = MagicMock()

    with patch(
        "drunc.process_manager.interface.commands.escape", side_effect=lambda x: x
    ):
        result = CliRunner().invoke(
            logs,
            ["--name", "process1", "--how-far", "50", "--grep", "keyword"],
            obj=mock_context,
        )

        assert result.output == ""
        assert result.exit_code == 0


def test_restart():
    """
    Test the restart command.
    """

    mock_driver = MockDriver()
    mock_driver.restart = MagicMock()
    mock_context = MockContext(driver=mock_driver)

    dummy_restart_arguments = ["--name", "process1"]
    result = CliRunner().invoke(restart, dummy_restart_arguments, obj=mock_context)

    assert result.exit_code == 0
    mock_driver.restart.assert_called_once()


def test_ps_with_process_query(mock_tabulate):
    """
    Test the ps command with process query options.
    """

    mock_driver = MockDriver()
    mock_driver.ps = MagicMock()
    mock_context = MockContext(driver=mock_driver)

    dummy_ps_arguments = ["--name", "process1"]
    result = CliRunner().invoke(ps, dummy_ps_arguments, obj=mock_context)

    assert result.exit_code == 0
    mock_driver.ps.assert_called_once()


############################################################################
# Tests for Cli arguments
############################################################################


@pytest.mark.parametrize(
    "args, missing_arg",
    [
        ([], "CONFIGURATION_FILE"),
        (["config.yaml", "config_id_124"], "SESSION_NAME"),
        (["config.yaml"], "CONFIGURATION_ID"),
    ],
)
def test_boot_missing_positional_arguments(args, missing_arg):
    """
    Test boot command with missing positional arguments.
    """
    runner = CliRunner()
    result = runner.invoke(boot, args)
    assert result.exit_code != 0
    assert f"Missing argument '{missing_arg}'" in result.output


def test_dummy_boot_missing_session_name():
    """
    Test dummy_boot command with missing session-name argument.
    """
    runner = CliRunner()
    result = runner.invoke(dummy_boot, [])
    assert result.exit_code != 0
    assert "Missing argument 'SESSION_NAME'" in result.output


@pytest.mark.parametrize(
    "args, error_msg",
    [
        (["test-session", "--n-processes", "three"], "Invalid value for '-n'"),
        (["test-session", "--sleep", "ten"], "Invalid value for '-s'"),
        [["test-session", "--n_sleeps", "six"], "Invalid value for '--n_sleeps'"],
    ],
)
def test_dummy_boot_invalid_values(args, error_msg):
    """
    Test when str are passed to arguments that require int
    """
    runner = CliRunner()
    result = runner.invoke(dummy_boot, args)
    assert result.exit_code != 0
    assert error_msg in result.output


def test_kill_missing_required_arg():
    """
    Test the kill command with missing required argument.
    """
    runner = CliRunner()
    result = runner.invoke(kill, [], obj=MagicMock())

    assert result.exit_code != 0
    assert "Invalid value: You need to provide at least" in result.output


def test_logs_missing_required_arg():
    """
    Test the logs command with missing required argument.
    """
    runner = CliRunner()
    result = runner.invoke(logs, [], obj=MagicMock())

    assert result.exit_code != 0
    assert "Invalid value: You need to provide at least" in result.output


def test_restart_missing_required_arg():
    """
    Test the restart command with missing required argument.
    """
    runner = CliRunner()
    result = runner.invoke(restart, [], obj=MagicMock())

    assert result.exit_code != 0
    assert "Invalid value: You need to provide at least" in result.output
