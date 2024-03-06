from drunc.utils.configuration import ConfHandler

from enum import Enum

class ProcessManagerTypes(Enum):
    Unknown = 0
    SSH = 1
    K8s = 2

class ProcessManagerConfData:
    def __init__(self):
        self.broadcaster = None
        self.authoriser = None
        self.type = ProcessManagerTypes.Unknown
        self.command_address = ''


class ProcessManagerConfHandler(ConfHandler):

    def _parse_dict(self, data):
        new_data = ProcessManagerConfData()
        from drunc.broadcast.server.configuration import KafkaBroadcastSenderConfData
        new_data.broadcaster = KafkaBroadcastSenderConfData.from_dict(data['broadcaster'])
        new_data.authoriser = None

        match data['type'].lower():
            case 'ssh':
                new_data.type = ProcessManagerTypes.SSH
            case _:
                from drunc.process_manager.exceptions import UnknownProcessManagerType
                raise UnknownProcessManagerType(data['type'])

        new_data.command_address = data['command_address']

        return new_data