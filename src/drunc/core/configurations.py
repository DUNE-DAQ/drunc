
class PluginConfiguration:
    @staticmethod
    def get_from_dict(plugin_type, dict_config):
        return PluginConfiguration(
            ### TODO use moo here
            plugin_type = plugin_type,
            plugin_name = dict_config['name'],
            plugin_conf = dict_config.get('conf', {}),
        )

    
    def __init__(self, plugin_type, plugin_name, plugin_conf):
        self.plugin_type = plugin_type
        self.plugin_name = plugin_name
        self.plugin_conf = plugin_conf

    def name(self):
        return self.plugin_name

    def type(self):
        return self.plugin_type

    def conf(self):
        return self.plugin_conf
    
class SubControllerConfiguration:
    @staticmethod
    def get_from_jsonfile(filename):
        import json
        plugins = {}
        with open(filename, 'r') as f:
            configuration = json.load(f)
            for k,v in configuration['plugins'].items():
                plugins[k] = PluginConfiguration.get_from_dict(k, v)

        return SubControllerConfiguration(
            subcontroller_name = configuration['subcontroller']['name'],
            plugins = plugins,
        )

    def __init__(self, subcontroller_name, plugins):
        self.subcontroller_name = subcontroller_name
        self.plugins_conf = plugins

    def get_plugin_conf(self, plugin):
        return self.plugins_conf[plugin]

    def get_plugin_list(self):
        return self.plugins_conf.keys()

