from drunc.core.plugin import PluginFactory

class LoggerPluginFactory(PluginFactory):
    def __init__(self):
        super().__init__(plugin_type='logger')
        
    def get(self, configuration, controller_instance):
        self.check_plugin_type(configuration)

        name = configuration.plugin_name

        if   name == 'stdouterr':
            from .stdouterrplugin import StdOutErrPlugin
            return StdOutErrPlugin(configuration, controller_instance)
        # elif name == 'parser_db':   return .get(configuration)
        # elif name == 'parser_json': return .get(configuration)
        # elif name == 'pointer_db':  return .get(configuration)
        # elif name == 'pointer_dir': return .get(configuration)
        else:
            from drunc.core.plugin import PluginCreationError
            raise PluginCreationError(f'LoggerPlugin {name} not understood')
            
