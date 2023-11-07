

class ProcessManagerConfiguration:
    def __init__(self, configuration):
        import logging
        self.log = logging.getLogger('process-manager-configuration')
        self.data = configuration

    def get_authoriser_configuration(self):
        return self.data['authoriser']

    def get_broadcaster_configuration(self):
        return self.data['broadcaster']