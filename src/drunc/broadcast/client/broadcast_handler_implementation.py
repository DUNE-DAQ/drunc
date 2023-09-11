import abc



class BroadcastHandlerImplementation(abc.ABC):
    @abc.abstractmethod
    def stop(self):
        pass