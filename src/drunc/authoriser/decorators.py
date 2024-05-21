from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.generic_pb2 import PlainText

def authentified_and_authorised(action, system):

    def decor(cmd):

        import functools

        @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
        def check_token(obj, request):
            from logging import getLogger
            log = getLogger('authentified_and_authorised_decorator')
            log.debug('Entering')
            if not obj.authoriser.is_authorised(request.token, action, system, cmd.__name__):
                from drunc.authoriser.exceptions import Unauthorised
                return Response(
                    name = obj.name,
                    token = request.token,
                    data = PlainText(
                        text = f"User {request.token.user_name} is not authorised to execute {cmd.__name__} on {obj.name} (action type is {action}, system is {system})"
                    ),
                    flag = ResponseFlag.NOT_EXECUTED_NOT_AUTHORISED,
                    responses = []
                )

                # raise Unauthorised(
                #     user = request.token.user_name,
                #     action = action,
                #     command = cmd.__name__,
                #     drunc_system = obj.name,
                # )
            log.debug('Executing wrapped function')
            ret = cmd(obj, request)
            log.debug('Exiting')
            return ret
        return check_token

    return decor

def async_authentified_and_authorised(action, system):

    def decor(cmd):

        import functools

        @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
        async def check_token(obj, request):
            from logging import getLogger
            log = getLogger('authentified_and_authorised_decorator')
            log.debug('Entering')
            if not obj.authoriser.is_authorised(request.token, action, system, cmd.__name__):
                yield Response(
                    name = obj.name,
                    token = request.token,
                    data = PlainText(
                        text = f"User {request.token.user_name} is not authorised to execute {cmd.__name__} on {obj.name} (action type is {action}, system is {system})"
                    ),
                    flag = ResponseFlag.NOT_EXECUTED_NOT_AUTHORISED,
                    children = []
                )
            log.debug('Executing wrapped function')
            async for a in cmd(obj, request):
                yield a
            log.debug('Exiting')

        return check_token

    return decor