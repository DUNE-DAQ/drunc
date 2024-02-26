from enum import Enum
from drunc.exceptions import DruncSetupException

class ConfTypes(Enum):
    Unknown            = 0
    JsonFileName       = 1
    DAQConfDir         = 2
    RawDict            = 3
    ProtobufAny        = 4
    ProtobufObject     = 5
    OKSFileName        = 6
    OKSObject          = 7
    PyObject           = 8


class ConfData:
    type = ConfTypes.Unknown
    data = None
    def __init__(self, data, type):
        self.type = type
        self.data = data

    @staticmethod
    def get_from_url(url):
        from urllib.parse import urlparse
        u = urlparse(url)
        # urlparse("scheme://netloc/path;parameters?query#fragment")
        t = ConfTypes.Unknown
        match u.scheme:
            case 'file':
                t = ConfTypes.JsonFileName
            case 'oksconfig':
                t = ConfTypes.OKSFileName
            case _:
                raise DruncSetupException(f'{u.scheme} configuration type is not understood')

        if u.path: #ugly ugly ugly
            return ConfData(
                type = t,
                data = f'{u.netloc}/{u.path}'
            )
        else:
            return ConfData(
                type = t,
                data = f'{u.netloc}'
            )

class ConfTypeNotSupported(DruncSetupException):
    def __init__(self, conf_type, class_name):
        if not isinstance(class_name, str):
            class_name = type(class_name)
        message = f'\'{conf_type}\' is not supported by \'{class_name}\''
        super(ConfTypeNotSupported, self).__init__(message)


class ConfigurationHandler:
    def __init__(self, configuration:ConfData):
        from logging import getLogger
        self.log = getLogger(self.__class__.__name__)
        if not isinstance(configuration, ConfData):
            raise DruncSetupException(f'ConfigurationHandler expected "ConfData", got {type(configuration)}: {configuration}')

        self.conf = configuration

        self.validate_and_parse_configuration_location()

        self.log.info('Configured')

    def _parse_oks(self, oks_path):
        # Reimplement this in case you need to be able to parse OKS configurations
        raise ConfTypeNotSupported(ConfTypes.OKSFileName, self)

    def _parse_pbany(self, pbany_data):
        # Reimplement this in case you need to be able to parse OKS configurations
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self)

    def _parse_dict(self, data):
        # Reimplement to validate/parse the dictonary
        self.log.warning(f'The configuration passed for {type(self)} is just a raw dictionary, the information in it was not checked')
        return ConfTypes.RawDict, data


    def validate_and_parse_configuration_location(self):
        from os.path import exists

        match self.conf.type:
            case ConfTypes.OKSObject | ConfTypes.RawDict | ConfTypes.PyObject | ConfTypes.ProtobufObject:
                return

            case ConfTypes.JsonFileName:
                if not exists(self.conf.data):
                    raise DruncSetupException(f'Location {self.conf.data} is empty!')

                with open(self.conf.data) as f:
                    import json
                    data = json.loads(f.read())
                    self.conf.type, self.conf.data = self._parse_dict(data)

            case ConfTypes.OKSFileName:
                if not exists(self.conf.data):
                    raise DruncSetupException(f'Location {self.conf.data} is empty!')

                self.conf.data = self._parse_oks(self.conf.data)
                self.conf.type = ConfTypes.OKSObject

            case ConfTypes.ProtobufAny:
                self.conf.data = self._parse_pbany(self.conf.data)
                self.conf.type = ConfTypes.ProtobufObject

            case _:
                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")


    def get(self, obj):

        match self.conf.type:
            case ConfTypes.RawDict:
                return ConfData(
                    type = self.conf.type,
                    data = self.conf.data[obj],
                )
            case ConfTypes.OKSObject | ConfTypes.PyObject | ConfTypes.ProtobufObject:
                return ConfData(
                    type = self.conf.type,
                    data = getattr(self.conf.data, obj),
                )
            case ConfTypes.JsonFileName | ConfTypes.OKSFileName | ConfTypes.ProtobufAny:
                raise DruncSetupException(f'Configuration "{self.conf.data}" was not parsed, there is a setup error')
            case _:
                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")


    def get_raw(self, obj):

        match self.conf.type:
            case ConfTypes.RawDict:
                return self.conf.data[obj]
            case ConfTypes.OKSObject | ConfTypes.PyObject:
                return getattr(self.conf.data, obj)
            case ConfTypes.JsonFileName | ConfTypes.OKSFileName:
                raise DruncSetupException(f'Configuration in {self.conf.data} was not parsed, there is a setup error')
            case _:
                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")




