"""Configuration for the session manager service."""

from drunc.utils.configuration import (
    ConfData,
    ConfHandler,
    ConfTypeNotSupported,
    ConfTypes,
)


class SessionManagerConfData(ConfData):
    """Wrapper for session manager configuration data."""

    def __init__(self) -> None:
        pass

    def populate_from_dict(self, data: dict[str, object]) -> None:
        """Populate from dictionary data."""
        # Session manager has no configuration requirements
        pass

    def populate_from_pbany(self, pbany_data: object) -> None:
        """Populate from Protobuf Any message."""
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self.__class__.__name__)


class SessionManagerConfHandler(ConfHandler[SessionManagerConfData]):
    """Handler for session manager configuration."""

    confdata_cls = SessionManagerConfData
