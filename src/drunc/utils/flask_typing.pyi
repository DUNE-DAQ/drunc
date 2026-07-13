"""Typing stubs for Flask/Gunicorn-related types used by drunc.utils.flask_manager.

This module is intended to be imported only under ``TYPE_CHECKING`` to
avoid importing runtime dependencies during normal execution.
"""
from typing import Protocol

from flask import Flask

class _GunicornConfig(Protocol):
    settings: dict[str, object]

    def set(self, key: str, value: object) -> None: ...


class _BaseApplication:
    cfg: _GunicornConfig

    def __init__(self, *args: object, **kwargs: object) -> None: ...
    def run(self) -> None: ...


class _Resource: ...


class Api:
    def __init__(self, app: Flask) -> None: ...

    def add_resource(self, resource: type[_Resource], *urls: str, **kwargs: object) -> None: ...
