"""
These tests check that the current generated gRPC schema matches
the expected fields
"""
from druncschema.process_manager_pb2 import (
    ProcessRestriction,
    CommandNotificationMessage,
    GenericNotificationMessage,
    ExceptionNotification,
    LogRequest,
    LogLines,
    ProcessUUID,
    ProcessMetadata,
    ProcessQuery,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    BootRequest
)
from druncschema.token_pb2 import Token
from druncschema.request_response_pb2 import ResponseFlag


def test_process_restriction_field_init():
    """
    Test ProcessRestriction fields properly populated
    """
    hosts = ["host1", "host2"]
    host_types = ["worker", "manager"]
    restriction = ProcessRestriction(
        allowed_hosts=hosts,
        allowed_host_types=host_types
    )
    
    assert len(restriction.allowed_hosts) == 2
    assert len(restriction.allowed_host_types) == 2


def test_command_notification_message_field_init():
    """
    Test CommandNotificationMessage fields properly populated
    """
    user = "test_user"
    command = "test_command"
    notification = CommandNotificationMessage(
        user=user,
        command=command
    )
    
    assert notification.user == user
    assert notification.command == command


def test_generic_notification_message_field_init():
    """
    Test GenericNotificationMessage fields properly populated
    """
    message = "test message"
    notification = GenericNotificationMessage(
        message=message
    )
    
    assert notification.message == message


def test_exception_notification_stack_line_field_init():
    """
    Test ExceptionNotification.StackLine fields properly populated
    """
    line_text = "error line"
    line_number = "42"
    file = "test.py"
    
    stack_line = ExceptionNotification.StackLine(
        line_text=line_text,
        line_number=line_number,
        file=file
    )
    
    assert stack_line.line_text == line_text
    assert stack_line.line_number == line_number
    assert stack_line.file == file


def test_exception_notification_field_init():
    """
    Test ExceptionNotification fields properly populated
    """
    error_text = "test error"
    stack_trace = [
        ExceptionNotification.StackLine(
            line_text="line1",
            line_number="1",
            file="file1.py"
        ),
        ExceptionNotification.StackLine(
            line_text="line2",
            line_number="2",
            file="file2.py"
        )
    ]
    
    exception = ExceptionNotification(
        error_text=error_text,
        stack_trace=stack_trace
    )
    
    assert exception.error_text == error_text
    assert len(exception.stack_trace) == 2


def test_log_request_field_init():
    """
    Test LogRequest fields properly populated
    """
    token = Token()
    query = ProcessQuery()
    how_far = 100
    
    log_request = LogRequest(
        token=token,
        query=query,
        how_far=how_far
    )
    
    assert log_request.token == token
    assert log_request.query == query
    assert log_request.how_far == how_far


def test_log_lines_field_init():
    """
    Test LogLines fields properly populated
    """
    name = "test_process"
    token = Token()
    uuid = ProcessUUID(uuid="test-uuid")
    lines = ["line1", "line2"]
    flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    
    log_lines = LogLines(
        name=name,
        token=token,
        uuid=uuid,
        lines=lines,
        flag=flag
    )
    
    assert log_lines.name == name
    assert log_lines.token == token
    assert log_lines.uuid == uuid
    assert len(log_lines.lines) == 2
    assert log_lines.flag == flag


def test_process_uuid_field_init():
    """
    Test ProcessUUID fields properly populated
    """
    uuid = "test-uuid-123"
    process_uuid = ProcessUUID(uuid=uuid)
    
    assert process_uuid.uuid == uuid


def test_process_metadata_field_init():
    """
    Test ProcessMetadata fields properly populated
    """
    uuid = ProcessUUID(uuid="test-uuid")
    user = "test_user"
    session = "test_session"
    name = "test_process"
    hostname = "test_host"
    tree_id = "tree-123"
    
    metadata = ProcessMetadata(
        uuid=uuid,
        user=user,
        session=session,
        name=name,
        hostname=hostname,
        tree_id=tree_id
    )
    
    assert metadata.uuid == uuid
    assert metadata.user == user
    assert metadata.session == session
    assert metadata.name == name
    assert metadata.hostname == hostname
    assert metadata.tree_id == tree_id


def test_process_query_field_init():
    """
    Test ProcessQuery fields properly populated
    """
    token = Token()
    uuids = [ProcessUUID(uuid="uuid1"), ProcessUUID(uuid="uuid2")]
    names = ["name1", "name2"]
    user = "test_user"
    session = "test_session"
    
    query = ProcessQuery(
        token=token,
        uuids=uuids,
        names=names,
        user=user,
        session=session
    )
    
    assert query.token == token
    assert len(query.uuids) == 2
    assert len(query.names) == 2
    assert query.user == user
    assert query.session == session


def test_process_description_string_list_field_init():
    """
    Test ProcessDescription.StringList fields properly populated
    """
    values = ["value1", "value2"]
    string_list = ProcessDescription.StringList(values=values)
    
    assert len(string_list.values) == 2


def test_process_description_exec_and_args_field_init():
    """
    Test ProcessDescription.ExecAndArgs fields properly populated
    """
    exec = "/usr/bin/python"
    args = ["arg1", "arg2"]
    
    exec_and_args = ProcessDescription.ExecAndArgs(
        exec=exec,
        args=args
    )
    
    assert exec_and_args.exec == exec
    assert len(exec_and_args.args) == 2


def test_process_description_field_init():
    """
    Test ProcessDescription fields properly populated
    """
    metadata = ProcessMetadata(
        uuid=ProcessUUID(uuid="test-uuid"),
        user="test_user",
        name="test_process",
        hostname="test_host"
    )
    env = {"KEY1": "value1", "KEY2": "value2"}
    executable_and_arguments = [
        ProcessDescription.ExecAndArgs(
            exec="/usr/bin/python",
            args=["arg1", "arg2"]
        )
    ]
    process_execution_directory = "/tmp"
    process_logs_path = "/var/log"
    
    description = ProcessDescription(
        metadata=metadata,
        env=env,
        executable_and_arguments=executable_and_arguments,
        process_execution_directory=process_execution_directory,
        process_logs_path=process_logs_path
    )
    
    assert description.metadata == metadata
    assert len(description.env) == 2
    assert len(description.executable_and_arguments) == 1
    assert description.process_execution_directory == process_execution_directory
    assert description.process_logs_path == process_logs_path


def test_process_instance_field_init():
    """
    Test ProcessInstance fields properly populated
    """
    process_description = ProcessDescription()
    process_restriction = ProcessRestriction()
    status_code = ProcessInstance.StatusCode.RUNNING
    return_code = 0
    uuid = ProcessUUID(uuid="test-uuid")
    
    instance = ProcessInstance(
        process_description=process_description,
        process_restriction=process_restriction,
        status_code=status_code,
        return_code=return_code,
        uuid=uuid
    )
    
    assert instance.process_description == process_description
    assert instance.process_restriction == process_restriction
    assert instance.status_code == status_code
    assert instance.return_code == return_code
    assert instance.uuid == uuid


def test_process_instance_list_field_init():
    """
    Test ProcessInstanceList fields properly populated
    """
    name = "test_list"
    token = Token()
    values = [ProcessInstance(), ProcessInstance()]
    flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    
    instance_list = ProcessInstanceList(
        name=name,
        token=token,
        values=values,
        flag=flag
    )
    
    assert instance_list.name == name
    assert instance_list.token == token
    assert len(instance_list.values) == 2
    assert instance_list.flag == flag


def test_boot_request_field_init():
    """
    Test BootRequest fields properly populated
    """
    token = Token()
    process_description = ProcessDescription()
    process_restriction = ProcessRestriction()
    
    boot_request = BootRequest(
        token=token,
        process_description=process_description,
        process_restriction=process_restriction
    )
    
    assert boot_request.token == token
    assert boot_request.process_description == process_description
    assert boot_request.process_restriction == process_restriction