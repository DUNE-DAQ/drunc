import abc

from drunc.core.exception import DruncError

class PluginCreationError(DruncError):
    pass


class PluginFactory(metaclass=abc.ABCMeta):
    def __init__(self, plugin_type):
        self.plugin_type = plugin_type

    def check_plugin_type(self, configuration):
        if configuration.plugin_type != self.plugin_type:
            raise PluginCreationError(f'Wrong type of plugin, expected {self.plugin_type}, got {configuration_plugin_type}')

    @abc.abstractmethod
    def get(self, configuration, controller_instance):
        pass


class Plugin(metaclass=abc.ABCMeta):
    def __init__(self, configuration, controller_instance):
        self.configuration = configuration
        self.controller_instance = controller_instance

    @abc.abstractmethod
    def command_callback(self, command, data):
        pass
        
    @abc.abstractmethod
    def pre_command_callback(self, command, data):
        pass

    @abc.abstractmethod
    def post_command_callback(self, command, data):
        pass

    @abc.abstractmethod
    def status_callback(self):
        pass
