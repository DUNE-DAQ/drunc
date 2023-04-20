## import plugins.PluginA
## import plugins.pluginB


class FSMPluginFactory:
    def __init__(self):
        pass

    def get(self, plugin_name, configuration):
        if plugin_name == 'pluginA':
            return pluginA(configuration)
        elif plugin_name == 'pluginB':
            return pluginA(configuration)
        raise RuntimeError(f'{plugin_name} is not recognised!')


FSMPluginsFact = FSMPluginFactory()