"""
Dummy request objects for Session Manager endpoints.
"""

import google.protobuf.any_pb2
from druncschema.request_response_pb2 import Request
from druncschema.token_pb2 import Token

GENERIC_REQUEST = Request(
    token=Token(), data=google.protobuf.any_pb2.Any(value=b"test_data")
)
