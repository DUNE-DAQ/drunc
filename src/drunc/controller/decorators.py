
def in_control(cmd):

    from functools import wraps

    @wraps(cmd)
    def wrap(obj, request):
        if not obj.actor.token_is_current_actor(request.token):
            return Response(
                token = request.token,
                data = PlainText(
                    text = f"User {request.token.user_name} is not in control of {obj.__class__.__name__}",
                ),
                response_flag = ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
                response_children = {}
            )

        return cmd(obj, request)

    return wrap