
def test():
    from drunc.authoriser.dummy_authoriser import DummyAuthoriser
    from drunc.communication.controller_pb2 import Token
    import random, string
    
    da = DummyAuthoriser() # should always assert to True for any action
    for i in range(10):
        tok_str = ''.join(random.choice(string.ascii_letters) for il in range(i+10))
        token = Token(text = tok_str)
        
        action = ''.join(random.choice(string.ascii_letters) for il in range(i+10))
        
        assert da.is_authorised(token, action)
    
        assert da.authorised_actions(token) == []
