
def in_control(cmd):

    from functools import wraps

    @wraps(cmd)
    def wrap(obj, request):
        if not obj.actor.token_is_current_actor(request.token):
            from drunc.controller.exceptions import OtherUserAlreadyInControl
            actor = '' if obj.actor.get_token().token == '' else obj.actor.get_user_name()
            raise OtherUserAlreadyInControl(request.token.user_name, actor, f'{obj.name}.{obj.session}', obj.__class__.__name__)
        return cmd(obj, request)

    return wrap