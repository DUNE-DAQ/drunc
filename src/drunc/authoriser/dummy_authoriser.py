
from druncschema.token_pb2 import Token
from druncschema.authoriser_pb2 import ActionType, SystemType

# TODO: Should be communicating over network

# The Rolls Royce of the authoriser systems
class DummyAuthoriser:
    def __init__(self, configuration:dict, system:SystemType):
        import logging
        self.log = logging.getLogger("Controller")
        self.log.info(f'DummyAuthoriser ready')
        self.configuration = configuration
        self.command_actions = {} # Dict[str, ActionType]
        self.system = system


    def is_authorised(self, token:Token, action:ActionType, system:SystemType, cmd_name:str='') -> bool:
        self.log.debug(f'Authorising {token.user_name} to {ActionType.Name(action)} ({cmd_name}) on {SystemType.Name(system)}')
        return True


    def authorised_actions(self, token:Token) -> list[str]:
        self.log.info(f'Grabbing authorisations for {token.token}')
        return []


def main():
    a = DummyAuthoriser()

if __name__ == '__main__':
    main()
