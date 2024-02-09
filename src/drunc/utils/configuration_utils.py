from enum import Enum
from drunc.exceptions import DruncSetupException

class ConfTypes(Enum):
    Unknown            = 0
    JsonFileName       = 1
    DAQConfDir         = 2
    RawDict            = 3
    ProtobufSerialised = 4
    ProtobufObject     = 5
    OKSFileName        = 6
    OKSObject          = 7


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
        #urlparse("scheme://netloc/path;parameters?query#fragment")
        t = ConfTypes.Unknown
        match u.scheme:
            case 'file':
                t = ConfTypes.JsonFileName
            case 'oksconfig':
                t = ConfTypes.OKSFileName
            case _:
                raise DruncSetupException(f'{u.scheme} configuration type is not understood')
        return ConfData(
            type = t,
            data = f'{u.netloc}/{u.path}'
        )

class ConfTypeNotSupported(DruncSetupException):
    def __init__(self, conf_type, class_name):
        message = f'{conf_type.value} is not supported by {class_name}'
        super(ConfTypeNotSupported, self).__init__(message)


class ConfigurationHandler:
    def __init__(self, configuration:ConfData, schema=None):
        from logging import getLogger
        self.log = getLogger("configuration-handler")
        if not isinstance(configuration, ConfData):
            raise DruncSetupException(f'ConfigurationHandler expected "ConfData", got {type(configuration)}: {configuration}')

        self.conf = configuration
        self.schema = schema
        self.validate_and_parse_configuration_location()

        self.log.info('Configured')


    def validate_and_parse_configuration_location(self):
        from os.path import exists

        match self.conf.type:
            case ConfTypes.OKSObject | ConfTypes.RawDict:
                return

            case ConfTypes.JsonFileName:
                if not exists(self.conf.data):
                    raise DruncSetupException(f'Location {self.conf.data} is empty!')

                with open(self.conf.data) as f:
                    import json
                    self.conf.data = json.loads(f.read())
                    self.conf.type = ConfTypes.RawDict

            case ConfTypes.OKSFileName:
                if not exists(self.conf.data):
                    raise DruncSetupException(f'Location {self.conf.data} is empty!')

                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")

            case _:
                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")


    def get(self, obj):

        match self.conf.type:
            case ConfTypes.RawDict:
                return ConfData(
                    type = self.conf.type,
                    data = self.conf.data[obj]
                )
            case ConfTypes.OKSObject:
                return self.conf.data
            case ConfTypes.JsonFileName | ConfTypes.OKSFileName:
                raise DruncSetupException(f'Configuration in {self.conf.data} was not parsed, there is a setup error')
            case _:
                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")


    def get_raw(self, obj):

        match self.conf.type:
            case ConfTypes.RawDict:
                return self.conf.data[obj]
            case ConfTypes.OKSObject:
                return self.conf.data
            case ConfTypes.JsonFileName | ConfTypes.OKSFileName:
                raise DruncSetupException(f'Configuration in {self.conf.data} was not parsed, there is a setup error')
            case _:
                raise ConfTypeNotSupported(self.conf.type, "ControllerConfiguration")




