from drunc.fsm.fsm_core import FSMInterface
import requests


class LogbookInterface(FSMInterface):
    def __init__(self, configuration):
        super().__init__("logbook")

    def post_start(self, data):
        '''
        address = f"{self.host}:{self.port}/v1/elisaLogbook/message_on_start/"
        payload = {'apparatus_id': self.apparatus_id, 'author': credentials.user, 'message': message, 'run_num': self.run_num, 'run_type': self.run_type}
        myauth = (credentials.get_login("logbook").user, credentials.get_login("logbook").password)
        r = requests.post(address, data=payload, auth=myauth)
        '''
        print(f"Running post_start of {self.name}")

    def post_stop(self, data):
        '''
        address = f"{self.host}:{self.port}/v1/elisaLogbook/message_on_start/"
        payload = {'apparatus_id': self.apparatus_id, 'author': credentials.user, 'message': message, 'run_num': self.run_num, 'run_type': self.run_type}
        myauth = (credentials.get_login("logbook").user, credentials.get_login("logbook").password)
        r = requests.post(address, data=payload, auth=myauth)
        '''
        print(f"Running post_stop of {self.name}")