
class PluginFactory:
    @staticmethod
    def get(configuration, instance):
        pt = configuration.plugin_type

        if pt == 'logger':
            from duneruncontrol.plugins.logger.plugin import LoggerPluginFactory
            return LoggerPluginFactory.get(configuration, instance)

        elif pt == 'configuration':
            from duneruncontrol.plugins.configuration.pluginfactory import ConfigurationPluginFactory
            return ConfigurationPluginFactory.get(configuration, instance)

        elif pt == 'fsm':
            from duneruncontrol.plugins.fsm.pluginfactory import FSMPluginFactory
            return FSMPluginFactory.get(configuration, instance)

        elif pt == 'childcontrol':
            from duneruncontrol.plugins.childcontrol.pluginfactory import ChildControlPluginFactory
            return ChildControlPluginFactory.get(configuration, instance)

        elif pt == 'commandhandler':
            from duneruncontrol.plugins.commandhandler.pluginfactory import CommandHandlerPluginFactory
            return CommandHandlerPluginFactory.get(configuration, instance)

        elif pt == 'processmanager':
            from duneruncontrol.plugins.processmanager.pluginfactory import ProcessManagerPluginFactory
            return ProcessManagerPluginFactory.get(configuration, instance)

        else:
            from duneruncontrol.plugins.exceptions import PluginCreationError()
            raise PluginCreationError(f'The type {pt} isn\'t understood')
