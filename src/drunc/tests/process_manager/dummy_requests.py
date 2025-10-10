"""
Dummy request objects for Process Manager endpoints.
"""

import google.protobuf.any_pb2
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogRequest,
    ProcessDescription,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)
from druncschema.request_response_pb2 import Request
from druncschema.token_pb2 import Token

BOOT_REQUEST = BootRequest(
    token=Token(),
    process_description=ProcessDescription(
        metadata=ProcessMetadata(name="test_process")
    ),
    process_restriction=ProcessRestriction(),
)


PROCESS_QUERY_REQUEST = ProcessQuery(
    token=Token(),
    uuids=[ProcessUUID(uuid="test-uuid")],
    names=["test_process"],
    user="test_user",
    session="test_session",
)


GENERIC_REQUEST = Request(
    token=Token(), data=google.protobuf.any_pb2.Any(value=b"test_data")
)


LOG_REQUEST = LogRequest(
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
