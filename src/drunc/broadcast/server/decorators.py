

def broadcasted(cmd):

    import functools

    @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    def wrap(obj, request, context):
        from druncschema.broadcast_pb2 import BroadcastType
        from drunc.exceptions import DruncCommandException

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' attempting to execute \'{cmd.__name__}\'',
            btype = BroadcastType.ACK
        )

        ret = None

        try:
            ret = cmd(obj, request, context)

        except DruncCommandException as e:
            obj._interrupt_with_exception(
                exception = e,
                context = context
            )

        except Exception as e:
            import traceback
            obj._interrupt_with_exception(
                exception = e,
                stack = traceback.format_exc(),
                context = context
            )

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' successfully executed \'{cmd.__name__}\'',
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS
        )
        return ret

    return wrap



def async_broadcasted(cmd):

    import functools

    @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    async def wrap(obj, request, context):
        from druncschema.broadcast_pb2 import BroadcastType
        from drunc.exceptions import DruncCommandException

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' attempting to execute \'{cmd.__name__}\'',
            btype = BroadcastType.ACK
        )

        try:
            async for a in cmd(obj, request, context):
                yield a

        except DruncCommandException as e:
            await obj._async_interrupt_with_exception(
                exception = e,
                context = context
            )

        except Exception as e:
            import traceback
            await obj._async_interrupt_with_exception(
                exception = e,
                stack = traceback.format_exc(),
                context = context
            )

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' successfully executed \'{cmd.__name__}\'',
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS
        )

    return wrap
