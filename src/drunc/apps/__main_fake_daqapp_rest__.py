'''
This is a fake DAQ application that doesn't do anything, but should talk in the same way to the run control
'''

import argparse
import copy as cp
import logging
import random
import requests
import threading
import time
from urllib.parse import urlparse

from flask import Flask, Response, request, abort, redirect, jsonify, make_response
from flask_restful import Api, Resource


__version__='1.0.0'

# Logger
log = logging.getLogger('service_logger')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


class AppState:

    def __init__(self, app_name:str):
        self.appname = app_name
        self.state = 'INITIAL'
        self.executing_command = False


    def send_response(self, address:str, txt:str, success:bool=True, data:dict={}):
        try:
            requests.post(
                address,
                data = {
                    'success': success,
                    'result': txt,
                    'appname': self.appname,
                    'data': data,
                }
            )
        except Exception as e:
            log.error(f'Couldn\'t send response to response listener, reason \'{str(e)}\'')


    def execute_command(self, req_data, answer_port, answer_host, remote_host) -> Response:
        reply_address = f'{answer_host}:{answer_port}/response' if answer_host else f'{remote_host}:{answer_port}/response'

        entry_state = req_data['entry_state']
        exit_state  = req_data['exit_state']
        command_id  = req_data['id']
        data        = req_data.get('data', {})

        if self.executing_command:
            response_txt = 'Already executing a command!!'
            log.info(response_txt)
            self.send_response(
                address = reply_address,
                txt = response_txt,
                success = False,
            )
            return

        time_spent = data.get('execution-time', random.randint(1, 10))

        worries = random.randint(0, time_spent)

        if entry_state != '*' and self.state != entry_state.upper():
            info = f'DAQ Application is in state {self.state} and command {command_id} requires to be in state {entry_state.upper()} to execute. Not executing'
            log.info(info)
            self.send_response(
                success = False,
                address = reply_address,
                txt = info,
            )
            return

        log.info(f'Executing {command_id}')

        self.executing_command = True

        if data.get('seg_fault'):
            time.sleep(worries)
            info = '<seeeeeeeeeg fauuuuuuuult message>'
            log.info(info)
            self.send_response(
                success = False,
                address = reply_address,
                txt = info,
            )
            self.executing_command = False
            exit(data['seg_fault'])

        if data.get('throw'):
            time.sleep(worries)
            what = 'This is an eRrOr, YoU hAvE bEeN vErY nAuGhTy (aka task failed successfully)',
            log.info(what)
            self.send_response(
                success = False,
                address = reply_address,
                txt = what,
            )
            self.executing_command = False
            return



        time.sleep(time_spent)
        info = f'Executed {command_id} successfully'
        self.send_response(
            success = True,
            address = reply_address,
            txt = info,
        )
        self.state = exit_state.upper()
        self.executing_command = False
        return

app_state = AppState('unknown')

'''
Resources for Flask app
'''
class AppCommand(Resource):


    def post(self):

        try:
            data = request.get_json(force=True)
        except:
            return "Not a JSON command!\n", 406

        log.debug(f'GET request with args: {data}')
        thread = threading.Thread(
            target = app_state.execute_command,
            kwargs = {
                'req_data'   : cp.deepcopy(data),
                'answer_port': request.headers['X-Answer-Port'],
                'answer_host': request.headers.get('X-Answer-Host'),
                'remote_host': request.remote_addr,
            }
        )
        thread.start()

        return "Command received\n", 202

'''
Main flask app
'''
app = Flask(__name__)

api = Api(app)
api.add_resource(AppCommand, "/command", methods=['POST'])

@app.route('/')
def index():
  return f'Fake DAQ app v{__version__}'

def main():

    parser = argparse.ArgumentParser(
        prog = 'FakeApplication',
        description = 'This is a fake application that communicate in the same way with the RunControl as the DAQApplication (thru REST)',
    )

    parser.add_argument('-n', '--name',                 required = True           , help = 'The name of the app in the response')
    parser.add_argument('-p', '--partition',            default  = 'global'       , help = 'This is a dummy argument in this case')
    parser.add_argument('-c', '--commandFacility',      required = True           , help = 'Where the fake app should get its command from')
    parser.add_argument('-i', '--informationService',   default  = 'stdout://flat', help = 'This is a dummy argument in this case')
    parser.add_argument('-d', '--configurationService', required = True           , help = 'This is a dummy argument in this case')

    args = parser.parse_args()

    app_state.app_name = args.name

    url = urlparse(args.commandFacility)
    if url.scheme != 'rest':
        raise RuntimeError(f'Couldn\'t start the fake flask app with commandFacility {args.commandFacility}')
    print(f"Starting FakeDAQ app on {url}, {url.hostname}:{url.port}")
    app.run(host=url.hostname, port=url.port, debug=True)

if __name__ == '__main__':
    main()
