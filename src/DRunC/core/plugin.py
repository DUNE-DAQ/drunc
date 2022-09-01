from abc import ABC

class Plugin(ABC):
    def __init__(self, configuration):
        self.configuration = configuration

    @abstractmethod
    def command_callback(self, command, data):
        pass
        
    @abstractmethod
    def pre_command_callback(self, command, data):
        pass

    @abstractmethod
    def post_command_callback(self, command, data):
        pass

    @abstractmethod
    def status_callback(self):
        pass
