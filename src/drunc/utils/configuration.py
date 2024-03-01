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
        case 'oksconfig':
            return ConfTypes.OKSFileName
        case _:
            raise DruncSetupException(f'{scheme} configuration type is not understood')

def parse_conf_url(url:str) ->tuple[ConfTypes,str]:
    from urllib.parse import urlparse
    u = urlparse(url)
    # urlparse("scheme://netloc/path;parameters?query#fragment")
    t = CLI_to_ConfTypes(u.scheme)

    if u.path: #ugly ugly ugly
        return f'{u.netloc}/{u.path}', t
    else:
        return f'{u.netloc}', t

class ConfTypeNotSupported(DruncSetupException):
    def __init__(self, conf_type:ConfTypes, class_name:str):
        if not isinstance(class_name, str):
            class_name = class_name.__class__.__name__
        message = f'\'{conf_type}\' is not supported by \'{class_name}\''
        super().__init__(message)

class OKSKey:
    def __init__(self, schema_file:str, class_name:str, uid:str):
        self.schema_file = schema_file
        self.class_name = class_name
        self.uid = uid

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
            import oksdbinterfaces
            dal = oksdbinterfaces.dal.module('x', self.oks_key.schema_file)
            db = oksdbinterfaces.Configuration("oksconfig:" + oks_path)
            return db.get_dal(
                class_name=self.oks_key.class_name,
                uid=self.oks_key.uid
            )

        except ImportError as e:
           raise DruncSetupException(f'OKS is not setup in this python environment, cannot parse OKS configurations') from e

        except KeyError as e:
           raise DruncSetupException(f'OKS params where not passed to theis ConfigurationHandler, cannot parse OKS configurations') from e


    def _post_process_oks(self):
        pass

    def _parse_pbany(self, pbany_data):
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self)


    def _parse_dict(self, data):
        raise ConfTypeNotSupported(ConfTypes.JsonFileName, self)


    def validate_and_parse_configuration_location(self, *args, **kwargs):
        from os.path import exists

        match self.initial_type:
            case ConfTypes.PyObject:
                self.data = self.initial_data
                self.type = self.initial_type
                self._post_process_oks(*args, **kwargs)

            case ConfTypes.JsonFileName:
                if not exists(self.initial_data):
                    raise DruncSetupException(f'Location {self.initial_data} is empty!')

                with open(self.initial_data) as f:
                    import json
                    data = json.loads(f.read())
                    self.data = self._parse_dict(data)
                    self.type = ConfTypes.PyObject

            case ConfTypes.OKSFileName:
                if not exists(self.initial_data):
                    raise DruncSetupException(f'Location {self.initial_data} is empty!')

                self.data = self._parse_oks_file(self.initial_data)
                self.type = ConfTypes.PyObject
                self._post_process_oks(*args, **kwargs)

            case ConfTypes.ProtobufAny:
                self.data = self._parse_pbany(self.initial_data)
                self.type = ConfTypes.PyObject

            case _:
                raise ConfTypeNotSupported(self.initial_type, self.class_name)