
class PluginFactory:
    @staticmethod
    def get(configuration, instance):
        pt = configuration.plugin_type
        if   pt == 'configuration':
            from duneruncontrol.plugins.configurationpluginfactory import ConfigurationPluginFactory
            return ConfigurationPluginFactory.get(configuration)

        elif pt == 'fsm':
            from duneruncontrol.plugins.fsmpluginfactory import FSMPluginFactory
            return FSMPluginFactory.get(configuration)

        elif pt == 'childcontrol':
            from duneruncontrol.plugins.childcontrolpluginfactory import ChildControlPluginFactory
            return ChildControlPluginFactory.get(configuration)

        elif pt == 'control':
            from duneruncontrol.plugins.controlpluginfactory import ControlPluginFactory
            return ControlPluginFactory.get(configuration, instance)

        elif pt == 'processmanager':
            from duneruncontrol.plugins.processmanagerpluginfactory import ProcessManagerPluginFactory
            return ProcessManagerPluginFactory.get(configuration)

        else:
            from duneruncontrol.plugins.exceptions import PluginCreationError()
            raise PluginCreationError(f'The type {pt} isn\'t understood')
