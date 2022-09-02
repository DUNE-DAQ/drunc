from .loggerplugin import LoggerPlugin

class StdOutErrPlugin(LoggerPlugin):
    def __init__(self, configuration, controller_instance):
        super().__init__(configuration, controller_instance)

    def log(self, lvl, message):
        if lvl>=self.log_level:
            self.controller_instance.console.print(message)
