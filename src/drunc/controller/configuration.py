from typing import Optional, Dict, List


class ControllerConfiguration:
    def __init__(self, configuration_loc:str):
        from logging import getLogger
        self.log = getLogger("controller-configuration")
        self.configuration_loc = configuration_loc
        self.data = self.validate_configuration_location(configuration_loc)
        self.parse_configuration(self.data)
        self.log.info('Configured')

    def validate_configuration_location(self, configuration_loc:str) -> dict:
        from urllib.parse import urlparse
        loc = urlparse(configuration_loc)

        if loc.scheme == 'file':
            from os.path import exists
            if not exists(loc.netloc+loc.path):
                raise RuntimeError(f'Location {loc.netloc+loc.path} is empty!')

            conf_data = {}
            try:
                with open(loc.netloc+loc.path) as f:
                    import json
                    conf_data = json.loads(f.read())
                    return conf_data
            except Exception as e:
                raise RuntimeError(f'Couldn\'t parse configuration file {loc.netloc+loc.path}, cause {str(e)}') from e

        else:
            raise RuntimeError(f'Location scheme invalid {loc.scheme}')


    def parse_configuration(self, conf_data:dict) -> None:
        self.children_controllers = conf_data.get('children_controllers', [])
        self.applications = conf_data.get('apps', [])
        self.broadcast_receiving_port = conf_data.get('broadcast_receiving_port', 50051)
        self.authoriser = conf_data.get('authoriser', {})

    def get_authoriser_configuration(self):
        return self.data['authoriser']

    def get_broadcaster_configuration(self):
        return self.data['broadcaster']
