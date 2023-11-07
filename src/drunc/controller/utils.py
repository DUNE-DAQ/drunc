from drunc.controller.stateful_node import StatefulNode

def get_status_message(stateful:StatefulNode):
    from druncschema.controller_pb2 import Status
    state_string = stateful.get_node_operational_state()
    if state_string != stateful.get_node_operational_sub_state():
        state_string += f' ({stateful.get_node_operational_sub_state()})'

    return Status(
        name = stateful.name,
        state = state_string,
        sub_state = stateful.get_node_operational_sub_state(),
        in_error = stateful.node_is_in_error(),
        included = stateful.node_is_included(),
    )

