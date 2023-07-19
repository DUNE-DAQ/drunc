

class ProcessManagerConfiguration:
    def __init__(self, loc):
        import logging
        self.log = logging.getLogger('process-manager-configuration')

        self.configuration_loc = loc
        self.data = {
            "authoriser": {}
        }
