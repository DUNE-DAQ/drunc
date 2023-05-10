from drunc.utils.utils import get_logger

from druncschema.token_pb2 import Token
from druncschema.authoriser_pb2 import ActionType, SystemType, AuthoriserRequest

# TODO: Should be communicating over network

# The Rolls Royce of the authoriser systems
class DummyAuthoriser:
    def __init__(self, configuration:dict, system:SystemType):
        self.log = get_logger("Controller")
        self.log.info(f'DummyAuthoriser ready')
        self.configuration = configuration
        self.command_actions = {} # Dict[str, ActionType]
        self.system = system


    def is_authorised(self, token:Token, command:str) -> bool:
        action_type = self.command_actions.get(command, ActionType.UNSPECIFIED)
        self.log.info(f'Authorising {token.token} to {command} ({action_type})')
        return True


    def authorised_actions(self, token:Token) -> list[str]:
        self.log.info(f'Grabbing authorisations for {token.token}')
        return []


def main():
    a = DummyAuthoriser()
    print(a.is_authorised())

if __name__ == '__main__':
    main()
