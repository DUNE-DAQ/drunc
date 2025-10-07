"""
This module provides reusable fixtures for dummy requests and responses used
across multiple test files for the process manager to ensure the tests always
use the correct data structures.

If the serialisation tests fail, it is likely that the fixtures need to be updated
to be back in line with druncschema definitions.
"""

import google.protobuf.any_pb2
import pytest
from druncschema.description_pb2 import Description
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.token_pb2 import Token

# ============================================================================
# Request Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def boot_request():
    """
    Provide a standard BootRequest for testing boot endpoint behaviour.

    Returns:
        BootRequest: Request containing process description and restrictions
    """
    return BootRequest(
        token=Token(),
        process_description=ProcessDescription(
            metadata=ProcessMetadata(name="test_process")
        ),
        process_restriction=ProcessRestriction(),
    )


@pytest.fixture(scope="session")
def process_query_request():
    """
    Provide a standard ProcessQuery for testing endpoints that query processes.

    This request type is used by multiple endpoints (kill, restart, ps, flush)
    that need to identify specific processes by UUID, name, user, or session.

    Returns:
        ProcessQuery: Query containing process identification parameters
    """
    return ProcessQuery(
        token=Token(),
        uuids=[ProcessUUID(uuid="test-uuid")],
        names=["test_process"],
        user="test_user",
        session="test_session",
    )


@pytest.fixture(scope="session")
def generic_request():
    """
    Provide a generic Request for testing endpoints that accept any data.

    Returns:
        Request: Basic request containing token and arbitrary data payload
    """
    return Request(token=Token(), data=google.protobuf.any_pb2.Any(value=b"test_data"))


@pytest.fixture(scope="session")
def log_request():
    """
    Provide a LogRequest for testing log retrieval endpoint.

    Returns:
        LogRequest: Request containing process query and log line limit
    """
    return LogRequest(
        token=Token(),
        query=ProcessQuery(
            token=Token(),
            uuids=[ProcessUUID(uuid="test-uuid")],
            names=["test_process"],
            user="test_user",
            session="test_session",
        ),
        how_far=100,
    )


# ============================================================================
# Response Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def boot_response():
    """
    Provide a standard boot response containing a running process instance.

    Returns:
        ProcessInstanceList: Response with single running process
    """
    return ProcessInstanceList(
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


@pytest.fixture(scope="session")
def kill_response():
    """
    Provide a standard kill response indicating successful process termination.

    Returns:
        ProcessInstanceList: Empty response indicating processes were killed
    """
    return ProcessInstanceList(
        name="kill_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture(scope="session")
def restart_response():
    """
    Provide a standard restart response indicating successful process restart.

    Returns:
        ProcessInstanceList: Empty response indicating processes were restarted
    """
    return ProcessInstanceList(
        name="restart_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture(scope="session")
def ps_response():
    """
    Provide a standard ps response for process status queries.

    Returns:
        ProcessInstanceList: Empty response representing process status list
    """
    return ProcessInstanceList(
        name="ps_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture(scope="session")
def terminate_response():
    """
    Provide a standard terminate response indicating graceful shutdown.

    Returns:
        ProcessInstanceList: Empty response indicating manager termination
    """
    return ProcessInstanceList(
        name="terminate_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture(scope="session")
def logs_response():
    """
    Provide a standard logs response containing process log lines.

    Returns:
        LogLines: Response containing sample log entries for a process
    """
    return LogLines(
        name="logs_endpoint",
        token=Token(),
        uuid=ProcessUUID(uuid="test-uuid"),
        lines=["test log line 1", "test log line 2"],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture(scope="session")
def describe_response():
    """
    Provide a standard describe response containing manager metadata.

    Returns:
        Description: Response with process manager configuration details
    """
    return Description(
        type="process_manager",
        name="test_process_manager",
        info="/var/log/test",
        session="test_session",
        commands=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        token=Token(),
    )


@pytest.fixture(scope="session")
def flush_response():
    """
    Provide a standard flush response indicating successful process cleanup.

    Returns:
        ProcessInstanceList: Empty response indicating processes were flushed
    """
    return ProcessInstanceList(
        name="flush_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )
