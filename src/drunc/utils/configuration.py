"""Configuration utilities for DRUNC."""

import json
import logging
import os
from enum import Enum
from typing import Protocol, Self, cast

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


class _OksDbProtocol(Protocol):
    def get_dal(self, class_name: str, uid: str) -> object: ...


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


class ConfHandler:
    """Handler for loading and parsing DRUNC configurations.

    Supports multiple configuration sources via from_* classmethods.
    Subclasses override populate_from_dict / populate_from_pbany to handle
    JSON and protobuf sources, and _post_process_oks to handle OKS/pyobject
    sources (via self._raw_data).
    """

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
    _raw_data: object  # raw OKS/pyobject data, available during _post_process_oks

    @classmethod
    def from_pyobject(cls, data: object, session_name: str | None = None) -> Self:
        instance: ConfHandler = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = data
        instance._raw_data = data
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    @classmethod
    def from_pbany(cls, data: object, session_name: str | None = None) -> Self:
        instance: ConfHandler = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = data
        instance._raw_data = None
        instance.populate_from_pbany(data)
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    @classmethod
    def from_json(cls, path: str, session_name: str | None = None) -> Self:
        instance: ConfHandler = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = path
        resolved = expand_path(path, True)
        if not os.path.exists(expand_path(path)):
            raise DruncSetupException(f"Location {resolved} ({path}) is empty!")
        with open(resolved) as f:
            json_data = json.load(f)
        instance._raw_data = None
        instance.populate_from_dict(cast(dict[str, object], json_data))
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    @classmethod
    def from_oks(
        cls,
        url: str,
        oks_key: OKSKey,
        session_name: str | None = None,
    ) -> "ConfHandler":
        instance: ConfHandler = cls.__new__(cls)
        instance._init_common(session_name)
        instance.initial_data = url
        instance.oks_key = oks_key
        instance._raw_data = instance._parse_oks_file(url)
        instance.type = ConfTypes.PyObject
        instance._post_process_oks()
        return instance

    def populate_from_dict(self, data: dict[str, object]) -> None:
        """Populate from a dictionary (JSON source).

        Override in subclasses that support JSON configuration.
        """
        raise ConfTypeNotSupported(ConfTypes.JsonFileName, self.__class__.__name__)

    def populate_from_pbany(self, pbany_data: object) -> None:
        """Populate from a Protobuf Any message.

        Override in subclasses that support protobuf configuration.
        """
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self.__class__.__name__)

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
            return cast(_OksDbProtocol, self.db).get_dal(
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
        For OKS/pyobject sources, self._raw_data holds the raw object.
        """
        pass
