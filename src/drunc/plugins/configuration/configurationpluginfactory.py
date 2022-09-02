from drunc.core.plugin import PluginFactory

class ConfigurationPluginFactory(PluginFactory):
    def __init__(self):
        super().__init__(name='configuration')

    def get(self, configuration, subcontroller_instance):
        self.check_plugin_type(configuration)

        name = configuration.name

        if   name == 'parser_dir':
            from .parserdir import ParserDirPlugin
            return ParserDirPlugin(configuration, subcontroller_instance)
        # elif name == 'parser_db':   return .get(configuration)
        # elif name == 'parser_json': return .get(configuration)
        # elif name == 'pointer_db':  return .get(configuration)
        # elif name == 'pointer_dir': return .get(configuration)
        else:
            from drunc.core.plugin import PluginCreationError
            raise PluginCreationError(f'ConfigurationPlugin {name} not understood')
