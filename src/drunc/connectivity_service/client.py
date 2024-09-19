from requests.exceptions import HTTPError, ConnectionError
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
        self.logger = getLogger('ConnectivityServiceClient')

        if address.startswith('http://') or address.startswith('https://'):
            self.address = address
        else:
            # assume the simplest case here
            self.address = f'http://{address}'

    def retract(self, uid):
        from drunc.utils.utils import http_post
        data = {
            'partition': self.session,
            'connections': [
                {
                    'connection_id': uid,
                    'data_type': 'run-control-messages',
                }
            ]
        }
        for i in range(50):
            try:
                self.logger.info(f'Retracting \'{uid}\' on the application registry, attempt {i+1}')

                r = http_post(
                    self.address+"/retract",
                    data = data,
                    headers = {
                        'Content-Type': 'application/json'
                    },
                    as_json = True,
                    timeout = 0.5,
                    ignore_errors = True
                )
                self.logger.info(r.reason)
                r.raise_for_status()
                break
            except (HTTPError, ConnectionError) as e:
                from time import sleep
                sleep(0.5)
                continue



    def resolve(self, uid_regex:str) -> dict:
        from drunc.utils.utils import http_post
        data = {
            'data_type': 'run-control-messages',
            'uid_regex': uid_regex
        }
        for i in range(50):
            try:
                self.logger.info(f'Looking up \'{uid_regex}\' on the application registry, attempt {i+1}')
                response = http_post(
                    self.address + "/getconnection/" + self.session,
                    data = data,
                    headers = {
                        'Content-Type': 'application/json'
                    },
                    as_json = True,
                    timeout = 0.5,
                    ignore_errors = True
                )
                self.logger.info(response.reason)
                response.raise_for_status()
                return response.json()
            except (HTTPError, ConnectionError) as e:
                from time import sleep
                sleep(0.2)
                continue

        self.logger.critical(f'Could not find the address of \'{uid_regex}\' on the application registry')
        raise ApplicationLookupUnsuccessful


    def publish(self, uid, uri):
        from drunc.utils.utils import http_post
        for i in range(50):
            try:
                self.logger.info(f'Publishing \'{uid}\' on the application registry, attempt {i+1}')

                http_post(
                    self.address+"/publish",
                    data = {
                        'partition': self.session,
                        'connections':[
                            {
                                "connection_type": 0,
                                "data_type": "run-control-messages",
                                "uid": uid,
                                "uri": uri,
                            }
                        ]
                    },
                    headers = {
                        'Content-Type': 'application/json'
                    },
                    as_json= True,
                    timeout = 0.5,
                    ignore_errors = True
                ).raise_for_status()
                break
            except (HTTPError, ConnectionError) as e:
                from time import sleep
                sleep(0.2)
                continue