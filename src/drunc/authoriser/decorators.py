

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
                raise Unauthorised(
                    user = request.token.user_name,
                    action = action,
                    command = cmd.__name__,
                    drunc_system = obj.__class__.__name__,
                )
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
                from drunc.authoriser.exceptions import Unauthorised
                raise Unauthorised(
                    user = request.token.user_name,
                    action = action,
                    command = cmd.__name__,
                    drunc_system = obj.__class__.__name__,
                )
            log.debug('Executing wrapped function')
            async for a in cmd(obj, request):
                yield a
            log.debug('Exiting')

        return check_token

    return decor