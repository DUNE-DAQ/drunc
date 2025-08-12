"""
These tests check that the current generated gRPC schema matches
the expected fields
"""

from druncschema.process_manager_pb2 import ProcessRestriction

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
