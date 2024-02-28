from druncschema.controller_pb2 import Argument

class Transition:
    def __init__(self, name, source, destination, arguments=[], help:str=''):
        self.source = source
        self.destination = destination
        self.name = name
        self.arguments = arguments
        self.help = help

    def __eq__(self, another):
        same_name = hasattr(another, 'name') and self.name == another.name
        same_destination = hasattr(another, 'destination') and self.destination == another.destination
        same_source = hasattr(another, 'source') and self.source == another.source
        return same_name and same_destination and same_source

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return f'\"{self.name}\": \"{self.source}\" → \"{self.destination}\"'