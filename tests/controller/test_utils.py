import pytest

from drunc.controller.exceptions import DruncCommandException


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


def test_address_command():
    from druncschema.controller_pb2 import AddressedCommand
    from druncschema.generic_pb2 import PlainText
    from google.protobuf import any_pb2

    from drunc.controller.utils import address_command

    class MockNode:
        def __init__(self, name):
            self.name = name
            self.children_nodes = []

    obj = MockNode("root")
    obj.children_nodes = [MockNode(name="n0"), MockNode(name="n1")]

    obj.children_nodes[0].children_nodes = [MockNode(name="n00"), MockNode(name="n01")]
    obj.children_nodes[0].children_nodes[0].children_nodes = [
        MockNode(name="n000"),
        MockNode(name="n001"),
    ]
    obj.children_nodes[0].children_nodes[0].children_nodes[0].children_nodes = [
        MockNode(name="n0000"),
        MockNode(name="n0001"),
    ]
    obj.children_nodes[0].children_nodes[0].children_nodes[1].children_nodes = [
        MockNode(name="n0010"),
        MockNode(name="n0011"),
    ]
    obj.children_nodes[0].children_nodes[1].children_nodes = [
        MockNode(name="n010"),
        MockNode(name="n011"),
    ]

    obj.children_nodes[1].children_nodes = [MockNode(name="n10"), MockNode(name="n11")]
    obj.children_nodes[1].children_nodes[0].children_nodes = [
        MockNode(name="n100"),
        MockNode(name="n101"),
    ]
    obj.children_nodes[1].children_nodes[1].children_nodes = [
        MockNode(name="n110"),
        MockNode(name="n111"),
    ]

    command_data = any_pb2.Any()
    command_data.Pack(PlainText(text="some_command_data"))

    # Testing that root cannot be addressed
    assert address_command(obj, "some-command", command_data, "", False, False) == {}
    assert (
        address_command(obj, "some-command", command_data, "root", False, False) == {}
    )

    # Testing that execute_on_all_subsequent_children_in_path works
    ret = address_command(obj, "some-command", command_data, "", False, True)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
    }

    ret = address_command(
        obj=obj.children_nodes[0],
        command_name="some-command",
        command_data=command_data,
        target="n0",
        execute_along_path=False,
        execute_on_all_subsequent_children_in_path=True,
    )
    assert ret == {
        "n00": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n00",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
        "n01": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n01",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
    }

    # Testing that execute_on_all_subsequent_children_in_path works, with root specified
    ret = address_command(obj, "some-command", command_data, "root/n1", False, True)
    assert ret == {
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        )
    }
    ret = address_command(
        obj=obj.children_nodes[1],
        command_name="some-command",
        command_data=command_data,
        target="n1",
        execute_along_path=False,
        execute_on_all_subsequent_children_in_path=True,
    )
    assert ret == {
        "n10": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n10",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
        "n11": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n11",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
    }

    # Testing that one specific node can be addressed
    # ... with root
    ret = address_command(obj, "some-command", command_data, "root/n0", False, False)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )
    }
    ret = address_command(
        obj, "some-command", command_data, "root/n0/n00", False, False
    )
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n00",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )
    }
    ret = address_command(
        obj, "some-command", command_data, "root/n0/n00/n000", False, False
    )
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n00/n000",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )
    }

    # ... without root
    ret = address_command(obj, "some-command", command_data, "n0", False, False)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )
    }
    ret = address_command(obj, "some-command", command_data, "n0/n00", False, False)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n00",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )
    }
    ret = address_command(
        obj, "some-command", command_data, "n0/n00/n000", False, False
    )
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n00/n000",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )
    }

    # Testing that regex matching works
    # ... with root
    ret = address_command(obj, "some-command", command_data, "root/n.", False, False)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
    }
    ret = address_command(
        obj, "some-command", command_data, "root/n./n.0", False, False
    )
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n.0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1/n.0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
    }

    # ... without root
    ret = address_command(obj, "some-command", command_data, "n.", False, False)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
    }
    ret = address_command(obj, "some-command", command_data, "n./n.0", False, False)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n.0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1/n.0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        ),
    }

    # Checking that target can be prefixed with a slash
    ret = address_command(obj, "some-command", command_data, "/root", False, True)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
        "n1": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n1",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        ),
    }

    ret = address_command(obj, "some-command", command_data, "/root/n0", False, True)
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        )
    }
    ret = address_command(
        obj, "some-command", command_data, "/root/n0/n.0", False, True
    )
    assert ret == {
        "n0": AddressedCommand(
            command_name="some-command",
            command_data=command_data,
            target="n0/n.0",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        )
    }

    pytest.raises(
        DruncCommandException,
        address_command,
        obj,
        "some-command",
        command_data,
        "/n0",
        False,
        True,
    )
    ret = address_command(obj, "some-command", command_data, "N0", False, True)
    assert ret == {}
