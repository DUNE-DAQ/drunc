import pytest
from druncschema.request_response_pb2 import Description
from google.protobuf.json_format import MessageToDict

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.controller.controller_driver import ControllerDriver
from drunc.exceptions import DruncException
from drunc.utils.shell_utils import create_dummy_token_from_uname


def setup_controller_driver(processes_and_logs, dal, session_name) -> ControllerDriver:
    connectivity_service_port = dal.connectivity_service.service.port

    csc = ConnectivityServiceClient(
        session_name,
        f"localhost:{connectivity_service_port}",
    )

    token = create_dummy_token_from_uname()
    controller_address = None
    try:
        controller_address = csc.resolve(
            "controller-0_control", "RunControlMessage", ntries=20
        )
    except DruncException:
        pytest.fail("Controller did not advertise its address in time")

    controller_driver = ControllerDriver(
        controller_address[0]["uri"].replace("grpc://", ""),
        token,
    )

    return controller_driver


def compare_response_data(response, expected_response, name):
    if expected_response is None:
        assert response is None
        return

    response_dict = MessageToDict(response)
    expected_response_dict = MessageToDict(expected_response)

    assert response_dict.keys() == expected_response_dict.keys()

    for key, value in expected_response_dict.items():
        if value == "<name>":
            assert response_dict[key] == name
        elif value == "<endpoint>":
            assert response_dict[key].startswith("grpc://")
        else:
            assert response_dict[key] == value


def get_response_recursive(response, target):
    if target in ["", response.name]:
        return response

    for child in response.children:
        result = get_response_recursive(child, target)
        if result is not None:
            return result


def get_response_along_path(response, target_chain, response_chain):
    if response.name in target_chain:
        response_chain.append(response)
        return response_chain

    for child in response.children:
        result = get_response_along_path(child, target_chain, response_chain)
        if result is not None:
            response_chain.append(result)
            return response_chain


def address_command_case_deep_segments_config(
    controller_driver,
    command,
    expected_response,
    target,
    execute_along_path,
    execute_on_all_subsequent_children_in_path,
    command_kwargs,
):
    response = getattr(controller_driver, command)(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        **command_kwargs,
    )

    print("--------------------------------")
    print(f"{target=}")
    print(f"{execute_along_path=}")
    print(f"{execute_on_all_subsequent_children_in_path=}")

    target = "controller-0" if target == "" else target
    target_chain = target.split("/")

    if execute_along_path:
        response_chain = get_response_along_path(response, target_chain, [])
        for response, target_part in zip(response_chain, target_chain):
            assert response.data is not None
            assert response.name == target_part
            compare_response_data(response.data, expected_response, target_part)

    # response_recursive = get_response_recursive(response, target)
    # if execute_on_all_subsequent_children_in_path:
    #     assert response_recursive.data is not None
    #     compare_response_data(response_recursive.data, expected_response)


def address_commmand_permutations_deep_segments_config(
    controller_driver: ControllerDriver,
    command: str,
    expected_response=None,
    command_kwargs={},
):
    kwargs = {
        "controller_driver": controller_driver,
        "command": command,
        "expected_response": expected_response,
        "command_kwargs": command_kwargs,
    }

    kwargs["execute_along_path"] = False
    kwargs["execute_on_all_subsequent_children_in_path"] = False
    address_command_case_deep_segments_config(**kwargs, target="")
    address_command_case_deep_segments_config(**kwargs, target="controller-0")
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1"
    )
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1/controller-2"
    )

    kwargs["execute_along_path"] = False
    kwargs["execute_on_all_subsequent_children_in_path"] = True
    address_command_case_deep_segments_config(**kwargs, target="")
    address_command_case_deep_segments_config(**kwargs, target="controller-0")
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1"
    )
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1/controller-2"
    )

    kwargs["execute_along_path"] = True
    kwargs["execute_on_all_subsequent_children_in_path"] = False
    address_command_case_deep_segments_config(**kwargs, target="")
    address_command_case_deep_segments_config(**kwargs, target="controller-0")
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1"
    )
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1/controller-2"
    )

    kwargs["execute_along_path"] = True
    kwargs["execute_on_all_subsequent_children_in_path"] = True
    address_command_case_deep_segments_config(**kwargs, target="")
    address_command_case_deep_segments_config(**kwargs, target="controller-0")
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1"
    )
    address_command_case_deep_segments_config(
        **kwargs, target="controller-0/controller-1/controller-2"
    )


def test_controller_driver_init(one_controller_running):
    controller_driver = setup_controller_driver(*one_controller_running)
    description = controller_driver.describe()
    assert description is not None
    assert description.name == "controller-0"


def test_controller_driver_describe(many_controllers_running):
    controller_driver = setup_controller_driver(*many_controllers_running)
    address_commmand_permutations_deep_segments_config(
        controller_driver=controller_driver,
        command="describe",
        expected_response=Description(
            name="<name>",
            type="controller",
            endpoint="<endpoint>",
            session=many_controllers_running[2],
        ),
    )


# def test_controller_driver_describe_fsm(many_controllers_running):
#     controller_driver = setup_controller_driver(*many_controllers_running)

#     def check_transitions(description, expected_transitions):
#         transitions = [tr.name for tr in description.data.commands]
#         transitions.sort()
#         expected_transitions.sort()
#         assert transitions == expected_transitions


#     description = controller_driver.describe_fsm(key=None)
#     check_transitions(description, ["conf"])

#     description = controller_driver.describe_fsm(key="")
#     check_transitions(description, ["conf"])

#     description = controller_driver.describe_fsm(key="all-transitions")
#     check_transitions(description, ["conf", "scrap", "start", "enable_triggers", "disable_triggers", "drain_dataflow", "stop_trigger_sources", "stop", "change_rate"])


#     assert description.children[0].data.name == "controller-1"
#     controller_1_children = [c.data.name for c in description.children[0].children]
#     assert len(controller_1_children) == 2
#     assert "controller-2" in controller_1_children
#     assert "controller-4" in controller_1_children


#     description = controller_driver.describe_fsm(key="all-transitions")
#     assert description.data.name == "controller-0"
#     assert description.children[0].data.name == "controller-1"
#     controller_1_children = [c.data.name for c in description.children[0].children]
#     assert len(controller_1_children) == 2
#     assert "controller-2" in controller_1_children
#     assert "controller-4" in controller_1_children


#     # Test describe with target
#     description = controller_driver.describe_fsm(
#         target="controller-1",
#         execute_along_path=False,
#         execute_on_all_subsequent_children_in_path=True,
#     )
#     assert description.name == "controller-0"
#     assert description.data is None
#     assert len(description.children) == 1
#     assert description.children[0].name == "controller-1"
#     assert len(description.children[0].children) == 2
#     controller_1_children = [c.name for c in description.children[0].children]
#     assert "controller-2" in controller_1_children
#     assert "controller-4" in controller_1_children

#     # Test describe with execute_along_path=True
#     description = controller_driver.describe_fsm(
#         target="controller-1",
#         execute_along_path=True,
#         execute_on_all_subsequent_children_in_path=True,
#     )
#     assert description.name == "controller-0"
#     assert description.data is not None
#     assert len(description.children) == 1
#     assert description.children[0].name == "controller-1"
#     assert len(description.children[0].children) == 2
#     controller_1_children = [c.name for c in description.children[0].children]
#     assert "controller-2" in controller_1_children
#     assert "controller-4" in controller_1_children
