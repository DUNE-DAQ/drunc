"""Configuration utilities for DRUNC."""

import json
import logging
import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Protocol, Self, TypeVar, cast

import conffwk

from drunc.exceptions import DruncSetupException
from drunc.utils.utils import expand_path, get_logger


class ConfTypes(Enum):
    """Enumeration of supported configuration types."""

    Unknown = 0

    # End product
    PyObject = 1  # this is the OKS object under the hood, or something that "fakes" it

    # Raw types that need to be converted
    JsonFileName = 2
    ProtobufAny = 3
    OKSFileName = 4


def CLI_to_ConfTypes(scheme: str) -> ConfTypes:
    """Convert a CLI scheme string to a ConfTypes enum.

    Args:
        scheme: The scheme string ("file", "oksconflibs", or "").

    Returns:
        ConfTypes: The corresponding configuration type.

    Raises:
        DruncSetupException: If the scheme is not recognized.
    """
    match scheme:
        case "file":
            return ConfTypes.JsonFileName
        case "oksconflibs" | "":
            return ConfTypes.OKSFileName
        case _:
            raise DruncSetupException(f"{scheme} configuration type is not understood")


def parse_conf_url(url: str) -> tuple[str, ConfTypes]:
    """Parse a configuration URL into scheme and type.

    Args:
        url: The configuration URL (format: "scheme:filename").

    Returns:
        tuple[str, ConfTypes]: A tuple of (url, conf_type).
    """
    scheme, filename = url.split(":")
    t = CLI_to_ConfTypes(scheme)
    return url, t


class ConfigurationNotFound(DruncSetupException):
    """Exception raised when configuration is not found."""

    def __init__(self, requested_path: str) -> None:
        """Initialize the ConfigurationNotFound exception.

        Args:
            requested_path: The path to the configuration that was not found.
        """
        super().__init__(
            f"The configuration '{requested_path}' is not in $DUNEDAQ_DB_PATH, perhaps you forgot to 'dbt-workarea-env && dbt-build'?"
        )


class ConfTypeNotSupported(DruncSetupException):
    """Exception raised when a configuration type is not supported."""

    def __init__(self, conf_type: ConfTypes, class_name: str) -> None:
        """Initialize the ConfTypeNotSupported exception.

        Args:
            conf_type: The configuration type that is not supported.
            class_name: The name of the class where this type is not supported.
        """
        if not isinstance(class_name, str):
            class_name = class_name.__class__.__name__
        message = f"'{conf_type}' is not supported by '{class_name}'"
        super().__init__(message)


class OKSKey:
    """Key information for accessing OKS configuration objects."""

    def __init__(
        self, schema_file: str, class_name: str, obj_uid: str, session: str
    ) -> None:
        """Initialize an OKSKey.

        Args:
            schema_file: The OKS schema file path.
            class_name: The class name in the OKS schema.
            obj_uid: The unique identifier for the object.
            session: The session name.
        """
        self.schema_file = schema_file
        self.class_name = class_name
        self.obj_uid = obj_uid
        self.session = session


class _DataTypeName(Protocol):
    _name_: str


class _ConfigurationData(Protocol):
    type: _DataTypeName
    broadcaster: object
    authoriser: object


class ConfData(ABC):
    """Base class for configuration wrapper objects that populate from raw sources."""

    @abstractmethod
    def populate_from_dict(self, data: dict[str, object]) -> None:
        """Populate from a dictionary (JSON source).

        Args:
            data: Dictionary data from JSON configuration file.

        Raises:
            ConfTypeNotSupported: If this handler doesn't support JSON sources.
        """
        raise ConfTypeNotSupported(ConfTypes.JsonFileName, self.__class__.__name__)

    @abstractmethod
    def populate_from_pbany(self, pbany_data: object) -> None:
        """Populate from a Protobuf Any message.

        Args:
            pbany_data: Protobuf Any message.

        Raises:
            ConfTypeNotSupported: If this handler doesn't support Protobuf sources.
        """
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self.__class__.__name__)


ConfDataType = TypeVar("ConfDataType", bound=ConfData)


class ConfHandler(Generic[ConfDataType]):
    """Handler for loading and parsing DRUNC configurations.

    Generic over a ConfDataType that wraps the parsed configuration.
    Supports multiple configuration sources via from_* classmethods.
    """

    confdata_cls: type[ConfDataType]
    data: ConfDataType
    type: ConfTypes
    oks_key: OKSKey | None
    class_name: str
    log: logging.Logger
    root_id: int
    controller_id: int
    process_id: int
    process_id_infra: int
    session_name: str | None
    initial_data: object
    oks_path: str
    db: object

    @classmethod
    def from_pyobject(cls, data: object, session_name: str | None = None) -> Self:
        """Create handler from a Python object.

        Args:
            data: The configuration object (typically OKS DAL).
            session_name: Optional session name.

        Returns:
            Self: Initialized handler instance.
        """
        instance = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = data
        instance.data = cast(ConfDataType, data)
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    @classmethod
    def from_pbany(cls, data: object, session_name: str | None = None) -> Self:
        """Create handler from a Protobuf Any message.

        Args:
            data: The Protobuf Any message.
            session_name: Optional session name.

        Returns:
            Self: Initialized handler instance.

        Raises:
            ConfTypeNotSupported: If subclass doesn't override parsing.
        """
        instance = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = data
        instance.data = instance._new_conf_data()
        instance.data.populate_from_pbany(data)
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    @classmethod
    def from_json(cls, path: str, session_name: str | None = None) -> Self:
        """Create handler from a JSON file.

        Args:
            path: Path to JSON configuration file.
            session_name: Optional session name.

        Returns:
            Self: Initialized handler instance.

        Raises:
            DruncSetupException: If file not found.
            ConfTypeNotSupported: If subclass doesn't override parsing.
        """
        instance = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = path
        resolved = expand_path(path, True)
        if not os.path.exists(expand_path(path)):
            raise DruncSetupException(f"Location {resolved} ({path}) is empty!")
        with open(resolved) as f:
            json_data = json.loads(f.read())
            instance.data = instance._new_conf_data()
            instance.data.populate_from_dict(cast(dict[str, object], json_data))
            instance.type = ConfTypes.PyObject
            instance._post_process_oks()
        return instance

    @classmethod
    def from_oks(
        cls,
        url: str,
        oks_key: OKSKey,
        session_name: str | None = None,
    ) -> Self:
        """Create handler from OKS configuration.

        Args:
            url: OKS database path.
            oks_key: Key to identify the object in OKS.
            session_name: Optional session name.

        Returns:
            Self: Initialized handler instance.
        """
        instance = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = url
        instance.oks_key = oks_key
        instance.data = cast(ConfDataType, instance._parse_oks_file(url))
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    def _init_common(self, session_name: str | None = None) -> None:
        """Initialize common attributes.

        Args:
            session_name: Optional session name.
        """
        self.class_name = self.__class__.__name__
        self.log = get_logger("utils." + self.class_name)
        self.root_id = 0
        self.controller_id = 0
        self.process_id = 0
        self.process_id_infra = 0
        self.session_name = session_name
        self.oks_key = None
        self.type = ConfTypes.Unknown

    def get_data(self) -> ConfDataType:
        """Get the configuration data.

        Returns:
            ConfDataType: The stored configuration data.
        """
        return self.data

    def get_data_type_name(self) -> str:
        """Get the type name of the configuration data.

        Returns:
            str: The name of the data type.
        """
        return str(cast(_ConfigurationData, self.get_data()).type._name_)

    def get_data_broadcaster(self) -> object:
        """Get the broadcaster from the configuration data.

        Returns:
            object: The broadcaster object.
        """
        return cast(_ConfigurationData, self.get_data()).broadcaster

    def get_data_authoriser(self) -> object:
        """Get the authoriser from the configuration data.

        Returns:
            object: The authoriser object.
        """
        return cast(_ConfigurationData, self.get_data()).authoriser

    def copy_oks_key(self) -> OKSKey | None:
        """Get a copy of the OKS key if one exists.

        Returns:
            OKSKey | None: The OKS key, or None if not using OKS configuration.
        """
        return self.oks_key

    def _parse_oks_file(self, oks_path: str) -> object:
        """Parse OKS configuration file.

        Args:
            oks_path: Path to OKS database.

        Returns:
            object: The parsed DAL object.

        Raises:
            DruncSetupException: If OKS setup or parameters are missing.
        """
        try:
            self.oks_path = oks_path
            self.log.debug(f"Using {self.oks_path} to configure")
            self.db = conffwk.Configuration(self.oks_path)
            assert self.oks_key is not None, "OKS key is required for OKS configuration"
            return self.db.get_dal(
                class_name=self.oks_key.class_name, uid=self.oks_key.obj_uid
            )

        except ImportError as e:
            raise DruncSetupException(
                "OKS is not setup in this python environment, cannot parse OKS configurations"
            ) from e

        except KeyError as e:
            raise DruncSetupException(
                "OKS params where not passed to this ConfigurationHandler, cannot parse OKS configurations"
            ) from e

    def _post_process_oks(self) -> None:
        """Post-process configuration after loading.

        Override in subclasses to perform custom initialization.
        """
        pass

    def _new_conf_data(self) -> ConfDataType:
        """Create a new instance of the configuration data wrapper.

        Must be overridden by subclasses that use from_json or from_pbany.

        Returns:
            ConfDataType: A new empty instance ready for population.
        """
        return self.confdata_cls()
