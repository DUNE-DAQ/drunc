import pytest


def test_get_segment_lookup_timeout(load_test_config):
    from drunc.utils.configuration import parse_conf_url

    conf_path, conf_type = parse_conf_url("oksconflibs:deep-segments-config.data.xml")
    from drunc.controller.utils import get_segment_lookup_timeout

    try:
        import conffwk
    except ImportError:
        pytest.skip("conffwk not installed")

    db = conffwk.Configuration(conf_path)

    segment_0 = db.get_dal(class_name="Segment", uid="segment-0")
    assert get_segment_lookup_timeout(segment_0, base_timeout=60) == 60 * 5

    segment_1 = db.get_dal(class_name="Segment", uid="segment-1")
    assert get_segment_lookup_timeout(segment_1, base_timeout=60) == 60 * 4

    segment_2 = db.get_dal(class_name="Segment", uid="segment-2")
    assert get_segment_lookup_timeout(segment_2, base_timeout=60) == 60 * 2

    segment_3 = db.get_dal(class_name="Segment", uid="segment-3")
    assert get_segment_lookup_timeout(segment_3, base_timeout=60) == 60 * 1

    segment_4 = db.get_dal(class_name="Segment", uid="segment-4")
    assert get_segment_lookup_timeout(segment_4, base_timeout=60) == 60 * 3

    segment_5 = db.get_dal(class_name="Segment", uid="segment-5")
    assert get_segment_lookup_timeout(segment_5, base_timeout=60) == 60 * 2

    segment_6 = db.get_dal(class_name="Segment", uid="segment-6")
    assert get_segment_lookup_timeout(segment_6, base_timeout=60) == 60 * 1


def test_get_all_states():
    from druncschema.controller_pb2 import StatusResponse
    from druncschema.request_response_pb2 import ResponseFlag

    from drunc.controller.utils import get_all_states

    # Construct the expected StatusResponse object for the test, using the failure mode
    # testing session structure as an example
    # ftns* = failure testing nested segment
    # ftts* = failure testing top segment
    # * = a for fake_daq_application, c for drunc-controller
    # Nested segment 1
    test_status_response_ftns1a = StatusResponse(name="ft-nested-segment-1-application")
    test_status_response_ftns1a.status.state = "initial"
    test_status_response_ftns1a.status.sub_state = "idle"
    test_status_response_ftns1a.status.in_error = False
    test_status_response_ftns1a.status.included = True
    test_status_response_ftns1a.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_ftns1c = StatusResponse(name="ft-nested-segment-1-controller")
    test_status_response_ftns1c.status.state = "initial"
    test_status_response_ftns1c.status.sub_state = "idle"
    test_status_response_ftns1c.status.in_error = False
    test_status_response_ftns1c.status.included = True
    test_status_response_ftns1c.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    test_status_response_ftns1c.children.add().CopyFrom(test_status_response_ftns1a)

    # Nested segment 2
    test_status_response_ftns2a = StatusResponse(name="ft-nested-segment-2-application")
    test_status_response_ftns2a.status.state = "disconnected"
    test_status_response_ftns2a.status.sub_state = "disconnected"
    test_status_response_ftns2a.status.in_error = False
    test_status_response_ftns2a.status.included = True
    test_status_response_ftns2a.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_ftns21a = StatusResponse(
        name="ft-nested-segment-2.1-application"
    )
    test_status_response_ftns21a.status.state = "initial"
    test_status_response_ftns21a.status.sub_state = "idle"
    test_status_response_ftns21a.status.in_error = False
    test_status_response_ftns21a.status.included = True
    test_status_response_ftns21a.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_ftns2c = StatusResponse(name="ft-nested-segment-1-controller")
    test_status_response_ftns2c.status.state = "initialising"
    test_status_response_ftns2c.status.sub_state = "initialising"
    test_status_response_ftns21a.status.in_error = False
    test_status_response_ftns2c.status.included = True
    test_status_response_ftns2c.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    test_status_response_ftns2c.children.add().CopyFrom(test_status_response_ftns2a)
    test_status_response_ftns2c.children.add().CopyFrom(test_status_response_ftns21a)

    # Top segment
    test_status_response_fttsa = StatusResponse(name="ft-top-segment-application")
    test_status_response_fttsa.status.state = "initial"
    test_status_response_fttsa.status.sub_state = "idle"
    test_status_response_fttsa.status.in_error = False
    test_status_response_fttsa.status.included = True
    test_status_response_ftns2c.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_fttsc = StatusResponse(name="ft-top-segment-controller")
    test_status_response_fttsc.status.state = "initialising"
    test_status_response_fttsc.status.sub_state = "initialising"
    test_status_response_fttsc.status.in_error = False
    test_status_response_fttsc.status.included = True
    test_status_response_fttsc.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    test_status_response_fttsc.children.add().CopyFrom(test_status_response_ftns1c)
    test_status_response_fttsc.children.add().CopyFrom(test_status_response_ftns2c)
    test_status_response_fttsc.children.add().CopyFrom(test_status_response_fttsa)

    # Test get_all_states function, assert that it returns the expected unique states
    # from the constructed StatusResponse object
    top_segment_states = list(set(get_all_states(test_status_response_fttsc)))
    assert top_segment_states == ["initialising", "disconnected", "initial"]


def test_count_processes_in_status_response():
    from druncschema.controller_pb2 import StatusResponse
    from druncschema.request_response_pb2 import ResponseFlag

    from drunc.controller.utils import count_processes_in_status_response

    # Construct the expected StatusResponse object for the test, using the failure mode
    # testing session structure as an example
    # ftns* = failure testing nested segment
    # ftts* = failure testing top segment
    # * = a for fake_daq_application, c for drunc-controller
    # Nested segment 1
    test_status_response_ftns1a = StatusResponse(name="ft-nested-segment-1-application")
    test_status_response_ftns1a.status.state = "initial"
    test_status_response_ftns1a.status.sub_state = "idle"
    test_status_response_ftns1a.status.in_error = False
    test_status_response_ftns1a.status.included = True
    test_status_response_ftns1a.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_ftns1c = StatusResponse(name="ft-nested-segment-1-controller")
    test_status_response_ftns1c.status.state = "initial"
    test_status_response_ftns1c.status.sub_state = "idle"
    test_status_response_ftns1c.status.in_error = False
    test_status_response_ftns1c.status.included = True
    test_status_response_ftns1c.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    test_status_response_ftns1c.children.add().CopyFrom(test_status_response_ftns1a)

    # Nested segment 2
    test_status_response_ftns2a = StatusResponse(name="ft-nested-segment-2-application")
    test_status_response_ftns2a.status.state = "disconnected"
    test_status_response_ftns2a.status.sub_state = "disconnected"
    test_status_response_ftns2a.status.in_error = False
    test_status_response_ftns2a.status.included = True
    test_status_response_ftns2a.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_ftns21a = StatusResponse(
        name="ft-nested-segment-2.1-application"
    )
    test_status_response_ftns21a.status.state = "initial"
    test_status_response_ftns21a.status.sub_state = "idle"
    test_status_response_ftns21a.status.in_error = False
    test_status_response_ftns21a.status.included = True
    test_status_response_ftns21a.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_ftns2c = StatusResponse(name="ft-nested-segment-1-controller")
    test_status_response_ftns2c.status.state = "initialising"
    test_status_response_ftns2c.status.sub_state = "initialising"
    test_status_response_ftns21a.status.in_error = False
    test_status_response_ftns2c.status.included = True
    test_status_response_ftns2c.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    test_status_response_ftns2c.children.add().CopyFrom(test_status_response_ftns2a)
    test_status_response_ftns2c.children.add().CopyFrom(test_status_response_ftns21a)

    # Top segment
    test_status_response_fttsa = StatusResponse(name="ft-top-segment-application")
    test_status_response_fttsa.status.state = "initial"
    test_status_response_fttsa.status.sub_state = "idle"
    test_status_response_fttsa.status.in_error = False
    test_status_response_fttsa.status.included = True
    test_status_response_ftns2c.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

    test_status_response_fttsc = StatusResponse(name="ft-top-segment-controller")
    test_status_response_fttsc.status.state = "initialising"
    test_status_response_fttsc.status.sub_state = "initialising"
    test_status_response_fttsc.status.in_error = False
    test_status_response_fttsc.status.included = True
    test_status_response_fttsc.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
    test_status_response_fttsc.children.add().CopyFrom(test_status_response_ftns1c)
    test_status_response_fttsc.children.add().CopyFrom(test_status_response_ftns2c)
    test_status_response_fttsc.children.add().CopyFrom(test_status_response_fttsa)

    # Test get_all_states function, assert that it returns the expected unique states
    # from the constructed StatusResponse object
    process_count = count_processes_in_status_response(test_status_response_fttsc)
    assert process_count == 7
