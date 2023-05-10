from drunc.utils.utils import get_logger


class ProcessManagerConfiguration:
    def __init__(self, loc):
        self.log = get_logger('process-manager-configuration')

        self.configuration_loc = loc
        self.data = {
            "authoriser": {}
        }
