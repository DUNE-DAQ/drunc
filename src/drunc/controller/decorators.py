
def in_control(cmd):

    from functools import wraps

    @wraps(cmd)
    def wrap(obj, request):
        if not obj.actor.token_is_current_actor(request.token):
            from druncschema.request_response_pb2 import Response, ResponseFlag
            from druncschema.generic_pb2 import PlainText
            from drunc.utils.grpc_utils import pack_to_any

            return Response(
                name = obj.name,
                token = request.token,
                data = pack_to_any(
                    PlainText(
                        text = f"User {request.token.user_name} is not in control of {obj.__class__.__name__}",
                    )
                ),
                flag = ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
                children = []
            )

        return cmd(obj, request)

    return wrap