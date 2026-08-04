"""
Dummy response objects for Process Manager endpoints
"""

from druncschema.description_pb2 import Description
from druncschema.process_manager_pb2 import (
    LogLines,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessUUID,
)
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.token_pb2 import Token

BOOT_RESPONSE = ProcessInstanceList(
    name="boot_endpoint",
    token=Token(),
    values=[
        ProcessInstance(
            uuid=ProcessUUID(uuid="test-boot-uuid"),
            status_code=ProcessInstance.StatusCode.RUNNING,
            return_code=0,
        )
    ],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)


KILL_RESPONSE = ProcessInstanceList(
    name="kill_endpoint",
    token=Token(),
    values=[],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)


RESTART_RESPONSE = ProcessInstanceList(
    name="restart_endpoint",
    token=Token(),
    values=[
        ProcessInstance(
            uuid=ProcessUUID(uuid="test-restart-uuid"),
            process_description=ProcessDescription(
                metadata=ProcessMetadata(hostname="test-host")
            ),
            status_code=ProcessInstance.StatusCode.RUNNING,
            return_code=0,
        )
    ],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)


PS_RESPONSE = ProcessInstanceList(
    name="ps_endpoint",
    token=Token(),
    values=[],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)


TERMINATE_RESPONSE = ProcessInstanceList(
    name="terminate_endpoint",
    token=Token(),
    values=[],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)


LOGS_RESPONSE = LogLines(
    name="logs_endpoint",
    token=Token(),
    uuid=ProcessUUID(uuid="test-uuid"),
    lines=["test log line 1", "test log line 2"],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)


DESCRIBE_RESPONSE = Description(
    type="process_manager",
    name="test_process_manager",
    info="/var/log/test",
    session="test_session",
    commands=[],
    children=[],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    token=Token(),
)


FLUSH_RESPONSE = ProcessInstanceList(
    name="flush_endpoint",
    token=Token(),
    values=[],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)
