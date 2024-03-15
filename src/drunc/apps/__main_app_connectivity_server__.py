__title__ = 'DAQ Application registry'
__author__ = 'Pierre Lasorak'
__credits__ = 'Gordon Crone, for his connectivity server'
__version__ = 'v0.0.0'
__maintainers__ = ['Pierre Lasorak']
__emails__ = ['plasorak@cern.ch']

from flask import Flask, request, make_response, jsonify
from flask_restful import Api, Resource
from threading import Lock
from datetime import datetime
import json

app = Flask(__name__)
api = Api(app)

lock = Lock()

toy_data_post = {
    "session": "session-name",
    "endpoints":[
        {
            "name": "app1-name",
            "endpoint": "rest://some-place:123"
        },
        {
            "name": "app2-name",
            "endpoint": "grpc://some-place:124"
        }
    ]
}
toy_data_post_str = json.dumps(toy_data_post, indent=2)
toy_data_post_html = []
toy_data_get = {
    "session": "session-name",
    "name": "app1-name",
}
toy_data_get_str = json.dumps(toy_data_get, indent=2)


class AppControlEndpoint:
    def __init__(self, session:str, name:str, endpoint:str):
        self.session = session
        self.name = name
        self.endpoint = endpoint
        self.last_updated = datetime.now()

    def __eq__(self, other):
        return self.session == other.session and self.name == other.name and self.endpoint == other.endpoint

    def get_last_time_heard(self):
        return self.last_updated.strftime("%Y/%m/%d-%H-%M-%S")

    def update(self, endpoint:str=None):
        if endpoint is not None:
            self.endpoint = endpoint
        self.last_updated = datetime.now()

class AppControlRegistry:
    def __init__(self):
        self.app_endpoints = {}


    def add_enpoint(self, endpoint:AppControlEndpoint):
        with lock:
            if not endpoint.session in self.app_endpoints:
                self.app_endpoints[endpoint.session] = []

            from copy import deepcopy as dc
            self.app_endpoints[endpoint.session] += [dc(endpoint)]


    def lookup(self, session:str, name:str='.*'):
        import re
        name_reg = re.compile(name)

        with lock:
            if session not in self.app_endpoints:
                return []

            ret = []
            for ep in self.app_endpoints[session]:
                if name_reg.fullmatch(ep.name):
                    ret += [ep]

            return ret


    def delete(self, session:str, name:str='.*'):
        import re
        name_reg = re.compile(name)

        with lock:
            if session not in self.app_endpoints:
                return

            from copy import deepcopy as dc
            endpoints = dc(self.app_endpoints[session])

            for ep in endpoints:
                if name_reg.fullmatch(ep.name) and ep in self.app_endpoints[session]:
                    self.app_endpoints[session].remove(ep)

            if self.app_endpoints[session]:
                del self.app_endpoints[session]
            return


acr = AppControlRegistry()
@api.resource(f"/app-registry/get-last-version")
class GetLastVersion(Resource):
    def get():
        return __version__

@api.resource(f"/app-registry/{__version__}/reset")
class Reset(Resource):
    def post():
        acr.app_endpoints = {}


@api.resource(f"/app-registry/{__version__}/app-control-connection")
class AppControlConnection(Resource):

    def post(self):
        session = None
        endpoints = []

        try:
            data = json.loads(request.data)
            session = data["session"]
            endpoints = data['endpoints']
        except Exception as e:
            return make_response(
                f'Missing field in request/invalid format: {str(e)}. Expected json data of format:\n{toy_data_post}',
                400
            )

        for endpoint in endpoints:
            try:
                url = endpoint["endpoint"]
                name = endpoint['name']
            except Exception as e:
                return make_response(
                    f'Missing field in request: {str(e)}. Expected json data of format:\n{toy_data_post}',
                    400
                )

            acr.add_enpoint(
                AppControlEndpoint(
                    name = name,
                    endpoint = url,
                    session = session
                )
            )


    def get(self):
        session = None
        name = None

        try:
            data = json.loads(request.data)
            session = data["session"]
            name = data["name"]
        except Exception as e:
            return make_response(
                f'Missing field in request/invalid format: {str(e)}. Expected json data of format:\n{toy_data_get}',
                400
            )

        connections = acr.lookup(session, name)
        if not connections:
            return make_response(f'No connection found with session={session} and name={name}', 404)
        return make_response(
            jsonify(
                [
                    {
                        'name': ac.name,
                        'session': ac.session,
                        'endpoint': ac.endpoint,
                        'last-time-heard': ac.get_last_time_heard(),
                    }
                    for ac in connections
                ]
            )
        )



    def delete(self):
        session = None
        name = None

        try:
            data = json.loads(request.data)
            session = data["session"]
            name = data["name"]
        except Exception as e:
            return make_response(
                f'Missing field in request/invalid format: {str(e)}. Expected json data of format:\n{toy_data_get}',
                400
            )

        acr.delete(session, name)

    def update(self):
        session = None
        name = None
        endpoint = None
        try:
            data = json.loads(request.data)
            session = data["session"]
            name = data["name"]
            endpoint = data.get('endpoint')
        except Exception as e:
            return make_response(
                f'Missing field in request/invalid format: {str(e)}. Expected json data of format:\n{toy_data_get}',
                400
            )

        connections = acr.lookup(session, name)
        if len(connections) > 1:
            return make_response(f'Too many connections found with session={session} and name={name}', 404)
        elif len(connections) == 1:
            return make_response(f'No connection found with session={session} and name={name}', 404)

        connections[0].update(endpoint)


@app.route("/")
def index():
    def format_endpoint(ep):
        return f'<p>{ep.name}: {ep.endpoint}, last update: {ep.last_updated}</p>'

    def format_session(session):
        return f'<h3>{session}</h3>\n'

    endpoint_pretty = ''
    for session, endpoint_list in acr.app_endpoints.items():
        endpoint_pretty += format_session(session)
        for endpoint in endpoint_list:
            endpoint_pretty += format_endpoint(endpoint)
        endpoint_pretty+="\n\n"

    root_text =f'''
<!DOCTYPE html>
<html>
<body>
<h1>{__title__}</h1>

<ul>
    <li>author: {__author__}</li>
    <li>credits: {__credits__}</li>
    <li>version: {__version__}</li>
    <li>maintainers: {__maintainers__}</li>
    <li>emails: {__emails__}</li>
</ul>

<h2>Running sessions</h2>
{endpoint_pretty}

<h2>Endpoints</h2>
<div style="border: 1px solid black">
<h3>POST /app-registry/get-last-version</h3>
<p>Returns the version of this service</p>
<p>Example:</p>
<p style="font-family:courier;">$ curl -X POST http://host:port/app-registry/get-last-version</p>
</div>
<p></p>

<div style="border: 1px solid black">
<h3>POST /app-registry/{__version__}/reset</h3>
<p>Reset this service (remove all the application connections)</p>
<p>Example:</p>
<p style="font-family:courier;">$ curl -X POST http://host:port/app-registry/{__version__}/reset</p>
</div>
<p></p>

<div style="border: 1px solid black">
<h3>GET /app-registry/{__version__}/app-control-connection</h3>
<p>Get the endpoint corresponding to the name/session pair. This function uses a form (!!) with of the format:</p>
<>
<p>Example:</p>
<p style="font-family:courier;">$ curl -X GET http://host:port/app-registry/{__version__}/app-control-connection</p>
</div>
<p></p>

<div style="border: 1px solid black">
<h3>GET /runregistry/getRunBlob/<run_num></h3>
<p>Get the run configuration blob (tar.gz of some folders structure containing json) for the specified run number (replace <run_num> by the run number you want).</p>
<p>Example:</p>
<p style="font-family:courier;">$ curl -u user:password -X GET -O -J http://host:port/runregistry/getRunBlob/2</p>
</div>
<p></p>

<div style="border: 1px solid black">
<h3>POST /runregistry/insertRun/</h3>
<p>Insert a new run in the database. The post request should have the fields:</p>
<ul>
    <li> "file": a file containing the configuration to save
    <li> "run_num": the run number
    <li> "det_id": the id of the detector
    <li> "run_type": the type of run (either PROD of TEST)
    <li> 'software_version": the version of dunedaq.
</ul>
<p>Example:</p>
<p style="font-family:courier;">$ curl -u user:password -F "file=@sspconf.tar.gz" -F "run_num=4" -F "det_id=foo" -F "run_type=bar" -F "software_version=dunedaq-vX.Y.Z" -X POST http://host:port/runregistry/insertRun/</p>
</div>
<p></p>

<div style="border: 1px solid black">
<h3>GET /runregistry/updateStopTime/<run_num></h3>
<p>Update the stop time of the specified run number (replace <run_num> with the run number you want).</p>
<p>Example:</p>
<p style="font-family:courier;">$ curl -u user:password -X GET http://host:port/runregistry/updateStopTime/2</p>
</div>
<p></p>

</body>
</html>
'''

    return root_text


if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=9826, debug=False)