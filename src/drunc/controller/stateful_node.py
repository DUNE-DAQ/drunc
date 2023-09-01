import abc


class StatefulNode(abc.ABC):
    def __init__(self, name, fsm):
        self.name = name
        self.fsm = fsm
        self.included = True

    def include(self):
        included = True

    def exclude(self):
        included = False

