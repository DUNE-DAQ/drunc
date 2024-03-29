
def in_control(cmd):

    from functools import wraps

    @wraps(cmd)
    def wrap(obj, request):
        if not obj.actor.token_is_current_actor(request.token):
            from drunc.controller.exceptions import OtherUserAlreadyInControl
            raise OtherUserAlreadyInControl()
        return cmd(obj, request)

    return wrap