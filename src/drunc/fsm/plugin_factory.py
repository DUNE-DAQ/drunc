## from interfaces.InterfaceA import InterfaceA
## import interfaces.interfaceB


class FSMInterfaceFactory:
    def __init__(self):
        pass

    def get(self, interface_name, configuration):
        if interface_name == 'interfaceA':
            return interfaceA(configuration)
        elif interface_name == 'interfaceB':
            return interfaceA(configuration)
        raise RuntimeError(f'{interface_name} is not recognised!')


FSMInterfacesFact = FSMInterfaceFactory()