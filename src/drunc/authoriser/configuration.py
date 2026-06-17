from drunc.utils.configuration import (
    ConfData,
    ConfHandler,
    ConfTypeNotSupported,
    ConfTypes,
)


class DummyAuthoriserConfData(ConfData):
    """Wrapper for dummy authoriser configuration data."""

    def __init__(self) -> None:
        pass

    def populate_from_dict(self, data: dict[str, object]) -> None:
        """Populate from dictionary data."""
        # Dummy authoriser has no configuration requirements
        pass

    def populate_from_pbany(self, pbany_data: object) -> None:
        """Populate from Protobuf Any message."""
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self.__class__.__name__)


class DummyAuthoriserConfHandler(ConfHandler[DummyAuthoriserConfData]):
    """Handler for dummy authoriser configuration."""

    confdata_cls = DummyAuthoriserConfData
