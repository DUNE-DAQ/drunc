from drunc.core.plugin import Plugin
import abc

class LoggerPlugin(Plugin):
    def __init__(self, configuration, controller_instance):
        super().__init__(configuration, controller_instance)

        from enum import IntEnum
        class LogLevels(IntEnum):
            FATAL = 1
            ERROR = 2
            INFO = 10
            DEBUG = 100

        self.log_level = configuration.conf()['loglevel']

    @abc.abstractmethod
    def log(self, lvl, message):
        pass

    def command_callback(self):
        pass

    def post_command_callback(self):
        pass

    def pre_command_callback(self):
        pass

    def status_callback(self):
        pass
