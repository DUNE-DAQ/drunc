

class ConfigurationManager:
    def __init__(self, configuration_loc:str):
        self.configuration_loc = configuration_loc
        conf_data = self.validate_configuration_location(configuration_loc)
        self.parse_configuration(conf_data)
        
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
        pass
        # TODO schema schema schema
        # self.children = conf_data['children']
