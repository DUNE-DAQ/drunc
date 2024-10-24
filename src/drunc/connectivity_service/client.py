from requests.exceptions import HTTPError, ConnectionError, ReadTimeout
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
                self.logger.debug(f'Retracting \'{uid}\' on the connectivity service, attempt {i+1}')

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

                if r.status_code == 404:
                    self.logger.warning(f'Connection \'{uid}\' not found on the application registry')
                    break

                r.raise_for_status()
                break
            except (HTTPError, ConnectionError) as e:
                from time import sleep
                sleep(0.5)
                continue



    def resolve(self, uid_regex:str, data_type:str) -> dict:
        from drunc.utils.utils import http_post
        data = {
            'data_type': data_type,
            'uid_regex': uid_regex
        }
        for i in range(50):
            try:
                self.logger.debug(f'Looking up \'{uid_regex}\' on the connectivity service, attempt {i+1}')
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
                response.raise_for_status()
                content = response.json()
                if content:
                    return content
                else:
                    self.logger.debug(f'Could not find the address of \'{uid_regex}\' on the application registry')

            except (HTTPError, ConnectionError, ReadTimeout) as e:
                self.logger.debug(e)
                from time import sleep
                sleep(0.2)
                continue

        self.logger.debug(f'Could not find the address of \'{uid_regex}\' on the application registry')
        raise ApplicationLookupUnsuccessful


    def publish(self, uid, uri, data_type:str):
        from drunc.utils.utils import http_post
        for i in range(50):
            try:
                self.logger.debug(f'Publishing \'{uid}\' on the connectivity service, attempt {i+1}')

                http_post(
                    self.address+"/publish",
                    data = {
                        'partition': self.session,
                        'connections':[
                            {
                                "connection_type": 0,
                                "data_type": data_type,
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
