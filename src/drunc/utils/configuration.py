"""Configuration utilities for DRUNC."""

import json
import os
from enum import Enum
from typing import Any

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


class ConfHandler:
    """Handler for loading and parsing DRUNC configurations.

    Supports multiple configuration types including JSON files, Protobuf messages, and OKS.
    """

    def __init__(
        self,
        data: Any = None,
        type: ConfTypes = ConfTypes.PyObject,
        oks_key: OKSKey | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a ConfHandler.

        Args:
            data: The configuration data. Defaults to None.
            type: The configuration type. Defaults to PyObject.
            oks_key: OKS key if using OKS configuration. Defaults to None.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.

        Raises:
            DruncSetupException: If OKS type is used without an OKS key.
        """
        self.class_name = self.__class__.__name__
        self.log = get_logger("utils." + self.class_name)
        self.initial_type = type
        self.initial_data = data
        self.root_id = 0
        self.controller_id = 0
        self.process_id = 0
        self.process_id_infra = 0
        self.session_name = kwargs.get("session_name")

        if type == ConfTypes.OKSFileName and oks_key is None:
            raise DruncSetupException("Need to provide a key for the OKS file")

        self.oks_key = oks_key
        self.validate_and_parse_configuration_location(*args, **kwargs)

    def get_data(self) -> Any:
        """Get the configuration data.

        Returns:
            Any: The stored configuration data.
        """
        return self.data

    def get_data_type_name(self) -> str:
        """Get the type name of the configuration data.

        Returns:
            str: The name of the data type.
        """
        return str(self.get_data().type._name_)

    def get_data_broadcaster(self) -> Any:
        """Get the broadcaster from the configuration data.

        Returns:
            Any: The broadcaster object.
        """
        return self.get_data().broadcaster

    def get_data_authoriser(self) -> Any:
        """Get the authoriser from the configuration data.

        Returns:
            Any: The authoriser object.
        """
        return self.get_data().authoriser

    def copy_oks_key(self) -> OKSKey | None:
        """Get a copy of the OKS key if one exists.

        Returns:
            OKSKey | None: The OKS key, or None if not using OKS configuration.
        """
        return self.oks_key

    def _parse_oks_file(self, oks_path: str) -> Any:
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

    def _post_process_oks(self, *args: Any, **kwargs: Any) -> None:
        pass

    def _parse_pbany(self, pbany_data: Any) -> Any:
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self.class_name)

    def _parse_dict(self, data: dict[str, Any]) -> Any:
        raise ConfTypeNotSupported(ConfTypes.JsonFileName, self.class_name)

    def validate_and_parse_configuration_location(
        self, *args: Any, **kwargs: Any
    ) -> None:
        """Validate and parse the configuration from the provided location.

        Supports JsonFileName, OKSFileName, and PyObject types.

        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        match self.initial_type:
            case ConfTypes.PyObject:
                self.data = self.initial_data
                self.type = self.initial_type
                self._post_process_oks(*args, **kwargs)

            case ConfTypes.JsonFileName:
                resolved = expand_path(self.initial_data, True)
                if not os.path.exists(expand_path(self.initial_data)):
                    raise DruncSetupException(
                        f"Location {resolved} ({self.initial_data}) is empty!"
                    )

                with open(resolved) as f:
                    data = json.loads(f.read())
                    self.data = self._parse_dict(data)
                    self.type = ConfTypes.PyObject
                    self._post_process_oks(*args, **kwargs)

            case ConfTypes.OKSFileName:
                self.data = self._parse_oks_file(self.initial_data)
                self.type = ConfTypes.PyObject
                self._post_process_oks(*args, **kwargs)

            case ConfTypes.ProtobufAny:
                self.data = self._parse_pbany(self.initial_data)
                self.type = ConfTypes.PyObject
                self._post_process_oks(*args, **kwargs)

            case _:
                raise ConfTypeNotSupported(self.initial_type, self.class_name)
