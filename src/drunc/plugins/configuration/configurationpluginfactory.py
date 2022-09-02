

class ConfigurationPluginFactory:
    @staticmethod
    def get(configuration):
        
        if configuration.plugin_type != 'configuration':
            from duneruncontrol.plugins.exceptions import PluginCreationError()
            raise PluginCreationError(f'Wrong type of plugin, expected configuration, got {configuration_plugin_type}')

        name = configuration.name
        from duneruncontrol.plugins.configuration.
        if   name == 'parser_dir':
            from duneruncontrol.plugins.configuration.parserdir import ParserDirPlugin
            return ParserDirPlugin(configuration)
        # elif name == 'parser_db':   return .get(configuration)
        # elif name == 'parser_json': return .get(configuration)
        # elif name == 'pointer_db':  return .get(configuration)
        # elif name == 'pointer_dir': return .get(configuration)
        else:
            from duneruncontrol.plugins.exceptions import PluginCreationError()
            raise PluginCreationError(f'ConfigurationPlugin {name} not understood')
            
