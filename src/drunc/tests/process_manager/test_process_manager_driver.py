from druncschema.process_manager_pb2 import ProcessRestriction


def test_valid_construction_empty():
    """
    Test ProcessRestriction construction with no arguments.
    
    Verifies that a ProcessRestriction instance created without parameters
    initializes with empty collections for both allowed_hosts and allowed_host_types.
    """
    restriction = ProcessRestriction()
    assert len(restriction.allowed_hosts) == 0
    assert len(restriction.allowed_host_types) == 0


def test_valid_construction_with_hosts():
    """
    Test ProcessRestriction construction with allowed_hosts parameter.
    
    Verifies that when a ProcessRestriction is created with a list of allowed hosts,
    the allowed_hosts field is properly populated and contains all expected values.
    """
    hosts = ["host1", "host2", "localhost"]
    restriction = ProcessRestriction(allowed_hosts=hosts)
    
    assert len(restriction.allowed_hosts) == 3
    assert "host1" in restriction.allowed_hosts
    assert "host2" in restriction.allowed_hosts
    assert "localhost" in restriction.allowed_hosts


def test_valid_construction_with_host_types():
    """
    Test ProcessRestriction construction with allowed_host_types parameter.
    
    Verifies that when a ProcessRestriction is created with a list of allowed host types,
    the allowed_host_types field is properly populated and contains all expected values.
    """
    host_types = ["worker", "manager", "controller"]
    restriction = ProcessRestriction(allowed_host_types=host_types)
    
    assert len(restriction.allowed_host_types) == 3
    assert "worker" in restriction.allowed_host_types
    assert "manager" in restriction.allowed_host_types
    assert "controller" in restriction.allowed_host_types


def test_valid_construction_with_both_fields():
    """
    Test ProcessRestriction construction with both hosts and host_types parameters.
    
    Verifies that when a ProcessRestriction is created with both allowed_hosts
    and allowed_host_types parameters, both fields are properly populated with
    the correct number of items.
    """
    hosts = ["host1", "host2"]
    host_types = ["worker", "manager"]
    restriction = ProcessRestriction(
        allowed_hosts=hosts,
        allowed_host_types=host_types
    )
    
    assert len(restriction.allowed_hosts) == 2
    assert len(restriction.allowed_host_types) == 2


def test_clear_field():
    """
    Test field clearing functionality using protobuf ClearField method.
    
    Verifies that the ClearField method properly clears a specific field
    while leaving other fields intact. Tests the selective clearing behavior
    of protobuf message fields.
    """
    restriction = ProcessRestriction(
        allowed_hosts=["host1"],
        allowed_host_types=["worker"]
    )
    
    # Clear only the allowed_hosts field
    restriction.ClearField("allowed_hosts")
    
    # Verify that allowed_hosts is cleared but allowed_host_types remains
    assert len(restriction.allowed_hosts) == 0
    assert len(restriction.allowed_host_types) == 1