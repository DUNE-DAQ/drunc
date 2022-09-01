
class PluginConfiguration:
    @staticmethod
    def get_from_dict(dict_config):
        instance = PluginConfiguration()
        instance.plugin_type(dict_config['type'])
        instance.plugin_name(dict_config['name'])
        instance.plugin_conf(dict_config['conf'])
        return instance
    
    def __init__(self):
        self.plugin_type = None
        self.plugin_name = None
        self.plugin_conf = None

    def name(self):
        return self.plugin_name

    def type(self):
        return self.plugin_type

    def conf(self):
        return self.plugin_conf
    
class SubControllerConfiguration:
    @staticmethod
    def get_from_jsonfile(filename):
        instance = SubControllerConfiguration()

        import json
        with open(filename, 'r') as f:
            configuration = json.load(f)
            instance.subcontroller_name = configuration['name']
            instance.subcontroller_type = configuration['type']
            for k,v in configuration['plugins']:
                instance.plugin_conf[k] = PluginConfiguration.get_from_dict(v)

    def __init__(self, json_config):
        self.subcontroller_type = None
        self.subcontroller_name = None
        self.plugin_conf = None

    def get_plugin_conf(self, plugin):
        return self.plugin_conf[plugin]

    def get_plugin_list(self):
        return [self.plugin_conf.keys()]
