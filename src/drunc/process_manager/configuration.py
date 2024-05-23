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
                new_data.kill_timeout = data.get("kill_timeout", 0.5)
            case 'k8s':
                new_data.type = ProcessManagerTypes.K8s
            case _:
                from drunc.process_manager.exceptions import UnknownProcessManagerType
                raise UnknownProcessManagerType(data['type'])

        new_data.command_address = data['command_address']

        return new_data



def get_cla(db, session_uid, obj):

    if hasattr(obj, "oksTypes"):
        if 'RCApplication' in obj.oksTypes():
            from coredal import rc_application_construct_commandline_parameters
            return rc_application_construct_commandline_parameters(db, session_uid, obj.id)

        elif 'SmartDaqApplication' in obj.oksTypes():
            from appdal import smart_daq_application_construct_commandline_parameters
            return smart_daq_application_construct_commandline_parameters(db, session_uid, obj.id)

        elif 'DaqApplication' in obj.oksTypes():
            from coredal import daq_application_construct_commandline_parameters
            return daq_application_construct_commandline_parameters(db, session_uid, obj.id)

    return obj.commandline_parameters