from drunc.core.pylogger import PyLogger
from drunc.communication.controller_pb2 import Token, ServerResponse



# TODO: Should be communicating over network

# The Rolls Royce of the authoriser systems
class DummyAuthoriser(PyLogger):
    def __init__(self):
        super(DummyAuthoriser, self).__init__('DummyAuthoriser')
        self.log.info(f'Ready')

    
    def is_authorised(self, token:Token, action:str) -> bool:
        self.log.info(f'Authorising {token.text} to {action}')
        return True
    

    def authorised_actions(self, token:Token) -> list[str]:
        self.log.info(f'Grabbing authorisations for {token.text}')
        return []


def main():
    a = DummyAuthoriser()
    print(a.is_authorised())
    
if __name__ == '__main__':
    main()
