import json
import os
from enum import Enum

import conffwk

from drunc.exceptions import DruncSetupException
from drunc.utils.utils import expand_path, get_logger


class ConfTypes(Enum):
    Unknown = 0

    # End product
    PyObject = 1  # this is the OKS object under the hood, or something that "fakes" it

    # Raw types that need to be converted
    JsonFileName = 2
    ProtobufAny = 3
    OKSFileName = 4


def CLI_to_ConfTypes(scheme: str) -> ConfTypes:
    match scheme:
        case "file":
            return ConfTypes.JsonFileName
        case "oksconflibs" | "":
            return ConfTypes.OKSFileName
        case _:
            raise DruncSetupException(f"{scheme} configuration type is not understood")


def parse_conf_url(url: str) -> tuple[str, ConfTypes]:
    scheme, filename = url.split(":")
    t = CLI_to_ConfTypes(scheme)
    return url, t


class ConfigurationNotFound(DruncSetupException):
    def __init__(self, requested_path):
        super().__init__(
            f"The configuration '{requested_path}' is not in $DUNEDAQ_DB_PATH, perhaps you forgot to 'dbt-workarea-env && dbt-build'?"
        )


class ConfTypeNotSupported(DruncSetupException):
    def __init__(self, conf_type: ConfTypes, class_name: str):
        if not isinstance(class_name, str):
            class_name = class_name.__class__.__name__
        message = f"'{conf_type}' is not supported by '{class_name}'"
        super().__init__(message)


class OKSKey:
    def __init__(self, schema_file: str, class_name: str, obj_uid: str, session: str):
        self.schema_file = schema_file
        self.class_name = class_name
        self.obj_uid = obj_uid
        self.session = session


class ConfHandler:
    def __init__(
        self,
        data=None,
        type=ConfTypes.PyObject,
        oks_key: OKSKey = None,
        *args,
        **kwargs,
    ):
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

    def get_data(self):
        return self.data

    def get_data_type_name(self):
        return self.get_data().type._name_

    def get_data_broadcaster(self):
        return self.get_data().broadcaster

    def get_data_authoriser(self):
        return self.get_data().authoriser

    def copy_oks_key(self):
        return self.oks_key

    def _parse_oks_file(self, oks_path):
        try:
            self.oks_path = oks_path
            self.log.debug(f"Using {self.oks_path} to configure")
            self.db = conffwk.Configuration(self.oks_path)
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

    def _post_process_oks(self):
        pass

    def _parse_pbany(self, pbany_data):
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self)

    def _parse_dict(self, data):
        raise ConfTypeNotSupported(ConfTypes.JsonFileName, self)

    def validate_and_parse_configuration_location(self, *args, **kwargs):
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


# def generate_fsm_command(ctx, transition: FSMCommandDescription, controller_name: str):
#     # Construct the base command
#     # run_one_fsm_command(controller, transition, target, **kwargs)
#     cmd = partial(run_one_fsm_command, controller_name, transition.name)
#     cmd = click.pass_obj(cmd)

#     # Standard target option
#     cmd = click.option(
#         "--target",
#         type=str,
#         help="The target to address",
#         default="",
#     )(cmd)

#     for argument in transition.arguments:
#         # Mapping gRPC types to Python types
#         type_map = {
#             Argument.Type.STRING: str,
#             Argument.Type.INT: int,
#             Argument.Type.FLOAT: float,
#             Argument.Type.BOOL: bool,
#         }

#         atype = type_map.get(argument.type)
#         if not atype:
#             raise Exception(f"Unhandled argument type '{argument.type}'")

#         # Extract Default Value
#         # Ensure we don't accidentally turn a 'None' into '0' or 'False'
#         # until we know if it's required.
#         raw_default = None
#         if argument.HasField("default_value"):
#             msg_map = {str: string_msg, int: int_msg, float: float_msg, bool: bool_msg}
#             unpacked = unpack_any(argument.default_value, msg_map[atype])
#             raw_default = atype(unpacked.value)

#         # Environment Variable Override
#         argument_name_cli = argument.name.lower().replace('_', '-')
#         env_var = f"DRUNC_{argument.name.upper()}_DEFAULT"
#         env_val = os.getenv(env_var)

#         if env_val is not None:
#             log.info(f"Env override for {argument_name_cli}: {env_val}")
#             default_value = atype(env_val)
#         else:
#             default_value = raw_default

#         # Logic Fix: A parameter is REQUIRED if the FSM says it is MANDATORY,
#         # regardless of whether the Python default is None.
#         is_required = (argument.presence == Argument.Presence.MANDATORY) and (default_value is None)

#         cmd = click.option(
#             f"--{argument_name_cli}",
#             type=atype,
#             default=default_value,
#             required=is_required,
#             show_default=True,
#             help=argument.help,
#         )(cmd)

#     cmd_name = format_name_for_cli(transition.name)

#     # Return as a Command object
#     return click.command(
#         name=cmd_name,
#         help=f"Execute {transition.name} on {controller_name}",
#     )(cmd)
