from druncschema.process_manager_pb2 import (
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessRestriction,
    ProcessUUID,
)
from druncschema.request_response_pb2 import ResponseFlag
from rich.console import Console

from drunc.process_manager.utils import tabulate_process_instance_list


def _make_process_instance(
    name: str,
    uuid: str,
    remote_pid: str | None = None,
    status_code: int = 0,
    return_code: int | None = 0,
) -> ProcessInstance:
    pi = ProcessInstance(
        process_description=ProcessDescription(
            metadata=ProcessMetadata(
                session="session-1",
                name=name,
                user="user-1",
                hostname="host-1",
                tree_id="0",
            ),
            executable_and_arguments=[
                ProcessDescription.ExecAndArgs(exec="/bin/sleep", args=["10"])
            ],
        ),
        process_restriction=ProcessRestriction(),
        status_code=status_code,
        uuid=ProcessUUID(uuid=uuid),
    )
    if return_code is not None:
        pi.return_code = return_code
    if remote_pid is not None:
        pi.remote_pid = remote_pid
    return pi


def test_tabulate_short_format_uses_status_without_exit_status_when_all_alive():
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-1", "uuid-1"),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=False)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "status" in rendered
    assert "Alive" in rendered
    assert "alive" not in rendered
    assert "exit-status" not in rendered


def test_tabulate_short_format_shows_exit_status_when_any_process_not_alive():
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-alive", "uuid-1"),
            _make_process_instance(
                "app-dead",
                "uuid-2",
                status_code=ProcessInstance.StatusCode.DEAD,
                return_code=7,
            ),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=False)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "status" in rendered
    assert "exit-status" in rendered
    assert "Dead" in rendered
    assert "7" in rendered


def test_tabulate_short_format_shows_not_available_when_exit_status_missing():
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance(
                "app-dead",
                "uuid-1",
                status_code=ProcessInstance.StatusCode.DEAD,
                return_code=None,
            ),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=False)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "exit-status" in rendered
    assert "Not available" in rendered


def test_tabulate_long_format_shows_remote_pid_column():
    """When at least one ProcessInstance has remote_pid set, the remote-pid column should appear."""
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-with-pid", "uuid-1", remote_pid="4242"),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=True)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "remote-pid" in rendered
    assert "4242" in rendered


def test_tabulate_long_format_shows_no_metadata_when_pid_is_reason():
    """When remote_pid contains a reason string it should appear as-is."""
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-no-meta", "uuid-2", remote_pid="no metadata"),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=True)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "remote-pid" in rendered
    assert "no metadata" in rendered


def test_tabulate_long_format_no_remote_pid_column_when_field_absent():
    """When no ProcessInstance has remote_pid set, the remote-pid column must not appear."""
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-1", "uuid-1"),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=True)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "remote-pid" not in rendered


def test_tabulate_short_format_never_shows_remote_pid_column():
    """The remote-pid column must not appear when long=False even if remote_pid is set."""
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-1", "uuid-1", remote_pid="1234"),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=False)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "remote-pid" not in rendered


def test_tabulate_long_format_executable_column_also_shown():
    """Executable column should still appear alongside remote-pid column."""
    process_list = ProcessInstanceList(
        name="pm",
        values=[
            _make_process_instance("app-1", "uuid-1", remote_pid="7777"),
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    table = tabulate_process_instance_list(process_list, title="test", long=True)

    console = Console(record=True, width=200)
    console.print(table)
    rendered = console.export_text()

    assert "executable" in rendered
    assert "/bin/sleep" in rendered
    assert "remote-pid" in rendered
    assert "7777" in rendered
