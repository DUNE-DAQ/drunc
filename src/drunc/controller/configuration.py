from drunc.exceptions import DruncSetupException

class ControllerConfiguration:
    def __init__(self, configuration_loc:str):
        from logging import getLogger
        self.log = getLogger("controller-configuration")

        from urllib.parse import urlparse
        self.cfg_loc = urlparse(configuration_loc)

        self.cfg_type = self.validate_configuration_location(self.cfg_loc)

        self.data = self.parse_configuration(self.cfg_loc)

        self.log.info('Configured')


    def validate_configuration_location(self, cfg_loc) -> dict:

        if cfg_loc.scheme == 'file':
            from os.path import exists
            if not exists(cfg_loc.netloc+cfg_loc.path):
                raise DruncSetupException(f'Location {cfg_loc.netloc+cfg_loc.path} is empty!')

        elif cfg_loc.scheme == 'oks':
            raise DruncSetupException(f'Configuration scheme invalid {cfg_loc.scheme}')

        else:
            raise DruncSetupException(f'Configuration scheme invalid {cfg_loc.scheme}')

        return cfg_loc.scheme

    def parse_configuration(self, cfg_loc) -> None:

        if cfg_loc.scheme == 'file':
            conf_data = {}
            try:
                with open(cfg_loc.netloc+cfg_loc.path) as f:
                    import json
                    conf_data = json.loads(f.read())
                    return conf_data
            except Exception as e:
                raise DruncSetupException(f'Couldn\'t parse configuration file {cfg_loc.netloc+cfg_loc.path}, cause {str(e)}') from e

        elif cfg_loc.scheme == 'oks':
            raise DruncSetupException(f'Configuration scheme invalid {cfg_loc.scheme}')

        else:
            raise DruncSetupException(f'Configuration scheme invalid {cfg_loc.scheme}')


    def get(self, obj, default=None):

        if self.cfg_type == 'file':
            return self.data.get(obj, default)

        elif self.cfg_type == 'oks':
            raise DruncSetupException(f'Configuration type invalid {self.cfg_type}')

