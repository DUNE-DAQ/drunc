import functools
from typing import Callable, Protocol, TypeVar, cast

import grpc
from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf.message import Message

from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import get_logger


class _AuthoriserProtocol(Protocol):
    def is_authorised(
        self,
        token: Token,
        action: ActionType.ValueType,
        system: SystemType.ValueType,
        cmd_name: str,
    ) -> bool: ...


# A Request protocol to a Servicer endpoint
# requires a token for authentication and authorisation.
class _RequestProtocol(Protocol):
    @property
    def token(self) -> Token: ...


# The required fields for a Servicer e.g. process manager, controller to
# be compatible with the authentified_and_authorised decorator.
class _ServicerProtocol(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def authoriser(self) -> _AuthoriserProtocol: ...


S = TypeVar("S", bound=_ServicerProtocol)
R = TypeVar("R", bound=_RequestProtocol)
M = TypeVar("M", bound=Message)

_ServicerRpcMethod = Callable[[S, R, grpc.ServicerContext], M]


def authentified_and_authorised(
    action: ActionType.ValueType, system: SystemType.ValueType
) -> Callable[[_ServicerRpcMethod[S, R, M]], _ServicerRpcMethod[S, R, M]]:
    """
    Decorator to check if the user is authentified and authorised to execute a command.

    Args:
        action: The action type to check.
        system: The system type to check.

    Returns:
        A decorator that checks if the user is authentified and authorised.

    Raises:
        Unauthorised: If the user is not authorised to execute the command.
    """

    def decor(cmd: _ServicerRpcMethod[S, R, M]) -> _ServicerRpcMethod[S, R, M]:
        def check_token(
            obj: S,
            request: R,
            context: grpc.ServicerContext,
        ) -> M:
            log = get_logger("utils.authentified_and_authorised_decorator")
            log.debug("Entering")
            if not obj.authoriser.is_authorised(
                request.token, action, system, cmd.__name__
            ):
                return cast(
                    M,
                    Response(
                        name=obj.name,
                        token=request.token,
                        data=pack_to_any(
                            PlainText(
                                text=f"User {request.token.user_name} is not authorised to execute {cmd.__name__} on {obj.name} (action type is {action}, system is {system})"
                            )
                        ),
                        flag=ResponseFlag.NOT_EXECUTED_NOT_AUTHORISED,
                        children=[],
                    ),
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

        return cast(
            _ServicerRpcMethod[S, R, M], functools.update_wrapper(check_token, cmd)
        )

    return decor
