
class PluginFactory:
    @staticmethod
    def get(configuration, instance):
        pt = configuration.plugin_type

        if pt == 'logger':
            from drunc.plugins.logger.pluginfactory import LoggerPluginFactory
            return LoggerPluginFactory().get(configuration, instance)

        elif pt == 'configuration':
            from drunc.plugins.configuration.pluginfactory import ConfigurationPluginFactory
            return ConfigurationPluginFactory().get(configuration, instance)

        elif pt == 'fsm':
            from drunc.plugins.fsm.pluginfactory import FSMPluginFactory
            return FSMPluginFactory().get(configuration, instance)

        elif pt == 'child-control':
            from drunc.plugins.childcontrol.pluginfactory import ChildControlPluginFactory
            return ChildControlPluginFactory().get(configuration, instance)

        elif pt == 'command-handler':
            from drunc.plugins.commandhandler.pluginfactory import CommandHandlerPluginFactory
            return CommandHandlerPluginFactory().get(configuration, instance)

        elif pt == 'process-manager':
            from drunc.plugins.processmanager.pluginfactory import ProcessManagerPluginFactory
            return ProcessManagerPluginFactory().get(configuration, instance)

        elif pt == 'user-authentication':
            from drunc.plugins.userauth.pluginfactory import UserAuthenticationPluginFactory
            return UserAuthenticationPluginFactory().get(configuration, instance)

        elif pt == 'run-number':
            from drunc.plugins.runnumber.pluginfactory import RunNumberPluginFactory
            return RunNumberPluginFactory().get(configuration, instance)

        elif pt == 'run-registry':
            from drunc.plugins.runregistry.pluginfactory import RunRegistryPluginFactory
            return RunRegistryPluginFactory().get(configuration, instance)

        elif pt == 'logbook':
            from drunc.plugins.logbook.pluginfactory import LogbookPluginFactory
            return LogbookPluginFactory().get(configuration, instance)

        elif pt == 'custom':
            from drunc.plugins.custom.pluginfactory import CustomPluginFactory
            return CustomPluginFactory().get(configuration, instance)

        else:
            from drunc.core.plugin import PluginCreationError
            raise PluginCreationError(f'The type {pt} isn\'t understood')
