import threading
from typing import Optional

from druncschema.token_pb2 import Token

from drunc.controller.exceptions import CannotSurrenderControl
from drunc.utils.utils import get_logger


class ControllerActor:
    def __init__(self, token: Optional[Token] = None):
        self.log = get_logger("controller.actor")
        self._token = Token(token="", user_name="")
        if token is not None:
            self._token.CopyFrom(token)
        self._lock = threading.Lock()

    def get_token(self) -> Token:
        return self._token

    def get_user_name(self) -> str:
        return self._token.user_name

    def _update_actor(self, token: Optional[Token] = None) -> None:
        self._lock.acquire()
        self._token = Token(token="", user_name="")
        if token is not None:
            self._token.CopyFrom(token)
        self._lock.release()

    def compare_token(self, token1, token2):
        self._lock.acquire()
        result = token1.user_name == token2.user_name and token1.token == token2.token
        self._lock.release()
        return result

    def token_is_current_actor(self, token):
        return self.compare_token(token, self._token)

    def surrender_control(self, token) -> None:
        if self.compare_token(self._token, token):
            self._update_actor(Token())
            return
        raise CannotSurrenderControl(
            f"Token {token} cannot release control of {self._token}"
        )

    def take_control(self, token) -> int:
        # if not self.compare_token(self._token, token):
        #     raise OtherUserAlreadyInControl(f'Actor {self._token.user_name} is already in control')
        self._update_actor(token)
        return 0
