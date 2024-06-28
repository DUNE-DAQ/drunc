from enum import Enum
from drunc.exceptions import DruncSetupException

class ConfTypes(Enum):
    Unknown = 0

    # End product
    PyObject = 1 # this is the OKS object under the hood, or something that "fakes" it

    # Raw types that need to be converted
    JsonFileName = 2
    ProtobufAny = 3
    OKSFileName = 4


def CLI_to_ConfTypes(scheme:str) -> ConfTypes:
    match scheme:
        case 'file':
            return ConfTypes.JsonFileName
        case 'oksconflibs':
            return ConfTypes.OKSFileName
        case _:
            raise DruncSetupException(f'{scheme} configuration type is not understood')

def parse_conf_url(url:str) ->tuple[ConfTypes,str]:
    from urllib.parse import urlparse
    u = urlparse(url)
    # urlparse("scheme://netloc/path;parameters?query#fragment")
    t = CLI_to_ConfTypes(u.scheme)

    if u.path: #ugly ugly ugly
        return f'{u.netloc}{u.path}', t
    else:
        return f'{u.netloc}', t





class ConfigurationNotFound(DruncSetupException):
    def __init__(self, requested_path):
        super().__init__(f'The configuration \'{requested_path}\' is not in $DUNEDAQ_DB_PATH, perhaps you forgot to \'dbt-workarea-env && dbt-build\'?')

def find_configuration(path:str) -> str:
    import logging
    log = logging.getLogger('find_configuration')
    import os
    from drunc.utils.utils import expand_path
    expanded_path = expand_path(path, turn_to_abs_path=False)
    if os.path.exists(expanded_path):
        return expanded_path

    configuration_files = []
    print(path)
    for dir in os.getenv('DUNEDAQ_DB_PATH').split(":"):
        tentative = os.path.join(dir, path)

        if os.path.exists(tentative):
            configuration_files += [tentative]

    if len(configuration_files)>1:
        l = "\n - ".join(configuration_files)
        log.warning(f'The configuration \'{path}\' matches >1 configurations in $DUNEDAQ_SHARED_PATH:\n - {l}\nUsing the first one')

    if not configuration_files:
        raise ConfigurationNotFound(path)

    return configuration_files[0]



class ConfTypeNotSupported(DruncSetupException):
    def __init__(self, conf_type:ConfTypes, class_name:str):
        if not isinstance(class_name, str):
            class_name = class_name.__class__.__name__
        message = f'\'{conf_type}\' is not supported by \'{class_name}\''
        super().__init__(message)

class OKSKey:
    def __init__(self, schema_file:str, class_name:str, obj_uid:str, session:str):
        self.schema_file = schema_file
        self.class_name = class_name
        self.obj_uid = obj_uid
        self.session = session

class ConfHandler:
    def __init__(self, data=None, type=ConfTypes.PyObject, oks_key:OKSKey=None, *args, **kwargs):
        from logging import getLogger
        self.class_name = self.__class__.__name__
        self.log = getLogger(self.class_name)
        self.initial_type = type
        self.initial_data = data

        if type == ConfTypes.OKSFileName and oks_key is None:
            raise DruncSetupException('Need to provide a key for the OKS file')

        self.oks_key = oks_key
        self.validate_and_parse_configuration_location(*args, **kwargs)

    def copy_oks_key(self):
        from copy import deepcopy as dc
        return self.oks_key

    def _parse_oks_file(self, oks_path):
        from drunc.exceptions import DruncSetupException

        try:
            import conffwk
            self.dal = conffwk.dal.module('x', self.oks_key.schema_file)
            self.oks_path = f"oksconflibs:{oks_path}"
            self.log.info(f'Using {self.oks_path} to configure')
            self.db = conffwk.Configuration(self.oks_path)
            return self.db.get_dal(
                class_name=self.oks_key.class_name,
                uid=self.oks_key.obj_uid
            )

        except ImportError as e:
           raise DruncSetupException(f'OKS is not setup in this python environment, cannot parse OKS configurations') from e

        except KeyError as e:
           raise DruncSetupException(f'OKS params where not passed to this ConfigurationHandler, cannot parse OKS configurations') from e


    def _post_process_oks(self):
        pass

    def _parse_pbany(self, pbany_data):
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self)


    def _parse_dict(self, data):
        raise ConfTypeNotSupported(ConfTypes.JsonFileName, self)


    def validate_and_parse_configuration_location(self, *args, **kwargs):
        from os.path import exists
        from drunc.utils.utils import expand_path

        match self.initial_type:


            case ConfTypes.PyObject:

                self.data = self.initial_data
                self.type = self.initial_type
                self._post_process_oks(*args, **kwargs)


            case ConfTypes.JsonFileName:

                resolved = expand_path(self.initial_data, True)
                if not exists(expand_path(self.initial_data)):
                    raise DruncSetupException(f'Location {resolved} ({self.initial_data}) is empty!')

                with open(resolved) as f:
                    import json
                    data = json.loads(f.read())
                    self.data = self._parse_dict(data)
                    self.type = ConfTypes.PyObject
                    self._post_process_oks(*args, **kwargs)


            case ConfTypes.OKSFileName:

                resolved = find_configuration(self.initial_data)
                if not exists(resolved):
                    raise DruncSetupException(f'Location {resolved} ({self.initial_data}) is empty!')

                self.data = self._parse_oks_file(resolved)
                self.type = ConfTypes.PyObject
                self._post_process_oks(*args, **kwargs)


            case ConfTypes.ProtobufAny:

                self.data = self._parse_pbany(self.initial_data)
                self.type = ConfTypes.PyObject
                self._post_process_oks(*args, **kwargs)


            case _:
                raise ConfTypeNotSupported(self.initial_type, self.class_name)



