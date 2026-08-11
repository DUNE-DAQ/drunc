import functools
from typing import Callable, Protocol

import grpc
from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token

from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import get_logger


class _AuthoriserProtocol(Protocol):
    def is_authorised(
        self,
        token: Token,
        action: ActionType,
        system: SystemType,
        command_name: str,
    ) -> bool: ...


class _RequestProtocol(Protocol):
    token: Token


class _ContextProtocol(Protocol):
    name: str
    authoriser: _AuthoriserProtocol


_Command = Callable[
    [_ContextProtocol, _RequestProtocol, grpc.ServicerContext], Response
]


def authentified_and_authorised(
    action: ActionType, system: SystemType
) -> Callable[[_Command], _Command]:
    def decor(cmd: _Command) -> _Command:
        @functools.wraps(
            cmd
        )  # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
        def check_token(
            obj: _ContextProtocol,
            request: _RequestProtocol,
            context: grpc.ServicerContext,
        ) -> Response:
            log = get_logger("utils.authentified_and_authorised_decorator")
            log.debug("Entering")
            if not obj.authoriser.is_authorised(
                request.token, action, system, cmd.__name__
            ):
                return Response(
                    name=obj.name,
                    token=request.token,
                    data=pack_to_any(
                        PlainText(
                            text=f"User {request.token.user_name} is not authorised to execute {cmd.__name__} on {obj.name} (action type is {action}, system is {system})"
                        )
                    ),
                    flag=ResponseFlag.NOT_EXECUTED_NOT_AUTHORISED,
                    children=[],
                )

                # raise Unauthorised(
                #     user = request.token.user_name,
                #     action = action,
                #     command = cmd.__name__,
                #     drunc_system = obj.name,
                # )
            log.debug("Executing wrapped function")
            ret = cmd(obj, request, context)
            log.debug("Exiting")
            return ret

        return check_token

    return decor
