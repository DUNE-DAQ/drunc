from requests.exceptions import HTTPError
from drunc.exceptions import DruncException

class ApplicationRegistryNotPresent(DruncException):
    pass

class ApplicationRegistrationUnsuccessful(DruncException):
    pass

class ApplicationLookupUnsuccessful(DruncException):
    pass

class ApplicationUpdateUnsuccessful(DruncException):
    pass


class ConnectivityServiceClient:
    def __init__(self, session:str, address:str):
        self.session = session
        from logging import getLogger
        self.logger = getLogger('ApplicationRegistryClient')

        if address.startswith('http'):
            self.address = f'{address}/app-registry/v0.0.0/app-control-connection'
        else:
            self.address = f'http://{address}/app-registry/v0.0.0/app-control-connection'

    def retract(self, uid):
        from drunc.utils.utils import http_post
        data = [{
            'partition': self.session,
            'connection_id': uid,
            'data_type': 'protobuf-control-messages',
        }]
        try:
            http_post(
                self.address+"/retract",
                data = data,
                headers = {
                    'Content-Type': 'application/json'
                },
                timeout = 0.5,
            )
        except Exception as e:
            self.logger.critical(f'Could not find the address of \'{name}\' on the application registry:\n{str(e)}')
            raise ApplicationLookupUnsuccessful from e




    def resolve(self, uid_regex:str) -> dict:
        from drunc.utils.utils import http_post
        data = {
            'session': self.session,
            'uid_regex': uid_regex
        }
        for _ in range(20):
            try:
                response = http_post(
                    self.address + "/getconnection/" + self.session,
                    data = data,
                    timeout = 0.5,
                )
                return response.json()
            except HTTPError as e:
                from time import sleep
                sleep(0.2)
                continue

        self.logger.critical(f'Could not find the address of \'{name}\' on the application registry')
        raise ApplicationLookupUnsuccessful


    def publish(self, uid, address):
        from drunc.utils.utils import http_post
        try:
            http_post(
                self.address+"/publish",
                data = {
                    'partition': self.session,
                    'connections':[
                        {
                            "connection_type": 0,
                            "data_type": "protobuf-control-messages",
                            "uid": name,
                            "uri": address,
                        }
                    ]
                },
                headers = {
                    'Content-Type': 'application/json'
                },
                timeout = 0.5,
            )

        except Exception as e: # do we still want to run if we cannot advertise ourselves?
            self.logger.critical(f'Could not post the address of self ({address}) on the application registry:\n{str(e)}')
            raise ApplicationRegistrationUnsuccessful from e
