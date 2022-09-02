from .status import Status
from drunc.plugins.pluginfactory import PluginFactory


class SubController:
    def __init__(self, configuration, console):
        self.console = console
        self.name = configuration.subcontroller_name
        self.configuration = configuration
        self.status = Status()

        ## All the plugins...
        self.plugins = {}
        plugins = self.configuration.get_plugin_list()
        for plugin in plugins:
            self.plugins[plugin] = PluginFactory.get(self.configuration.get_plugin_conf(plugin), self)

    def run(self):
        while True:
            self.console.print('.')
            from time import sleep
            sleep(1)
        # What it should actually do:
        # self.plugins['commandhandler'].run()

    def get_plugin(self, plugin_name):
        return self.plugin[plugin_name]

    def status_callback(self):
        for plugin_name, plugin in self.plugins.items():
            plugin.pre_commmand_callback(command, self.status)


    def pre_command_callback(self, command):
        data = {}

        for plugin_name, plugin in self.plugins.items():
            plugin.pre_commmand_callback(command, data)

        return data


    def post_command_callback(self, command):
        data = {}

        for plugin_name, plugin in self.plugins.items():
            plugin.post_commmand_callback(command, data)

        return data


    def command_callback(self, command):
        data = self.pre_command_callback(command)

        for plugin_name, plugin in self.plugins.items():
            plugin.command_callback(command, data)

        data = self.post_command_callback(command, data)

        return data
