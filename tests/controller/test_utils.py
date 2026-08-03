import pytest


@pytest.fixture
def mock_status_tree():
    """Builds and returns a mock StatusResponse tree for testing."""
    from druncschema.controller_pb2 import StatusResponse
    from druncschema.request_response_pb2 import ResponseFlag

    def create_status(
        name: str, state: str, sub_state: str, children: list = None
    ) -> StatusResponse:
        resp = StatusResponse(name=name, flag=ResponseFlag.EXECUTED_SUCCESSFULLY)
        resp.status.state = state
        resp.status.sub_state = sub_state
        resp.status.in_error = False
        resp.status.included = True
        if children:
            for child in children:
                resp.children.add().CopyFrom(child)
        return resp

    ftns1_app = create_status("ft-nested-segment-1-application", "initial", "idle")
    ftns1_ctrl = create_status(
        "ft-nested-segment-1-controller", "initial", "idle", [ftns1_app]
    )
    ftns2_app = create_status(
        "ft-nested-segment-2-application", "disconnected", "disconnected"
    )
    ftns21_app = create_status("ft-nested-segment-2.1-application", "initial", "idle")
    ftns2_ctrl = create_status(
        "ft-nested-segment-2-controller",
        "initialising",
        "initialising",
        [ftns2_app, ftns21_app],
    )
    ftts_app = create_status("ft-top-segment-application", "initial", "idle")

    return create_status(
        "ft-top-segment-controller",
        "initialising",
        "initialising",
        [ftns1_ctrl, ftns2_ctrl, ftts_app],
    )


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


# Now your tests become beautifully short:


def test_get_all_states(mock_status_tree):
    from drunc.controller.utils import get_all_states

    top_segment_states = set(get_all_states(mock_status_tree))
    assert top_segment_states == {"initialising", "disconnected", "initial"}


def test_count_processes_in_status_response(mock_status_tree):
    from drunc.controller.utils import count_processes_in_status_response

    process_count = count_processes_in_status_response(mock_status_tree)
    assert process_count == 7
