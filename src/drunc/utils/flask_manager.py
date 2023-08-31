import threading
import time
from typing import NoReturn
from multiprocessing import Process
import logging

class FlaskManager(threading.Thread):
    def __init__(self, name, app, port):
        threading.Thread.__init__(self)
        self.log = logging.getLogger(f"{name}-flaskmanager")
        self.name = name
        self.app = app
        self.flask = None
        self.port = port

        self.ready = False
        self.ready_lock = threading.Lock()

    def _create_flask(self) -> Process:
        need_ready = True
        need_shutdown = True

        for rule in self.app.url_map.iter_rules():
            if rule.endpoint == "readystatus":
                need_ready = False
            if rule.endpoint == "shutdown":
                need_shutdown = False


        def get_ready_status():
            return "ready"

        # no clue how to do that, so multiprocessing.Process.terminate it will be.
        # def shutdown():
        #     func = request.environ.get('werkzeug.server.shutdown')
        #     if func is None:
        #         raise RuntimeError('Not running with the Werkzeug Server')
        #     func()

        if need_ready:
            self.app.add_url_rule("/readystatus", "get_ready_status", get_ready_status, methods=["GET"])
        if need_shutdown:
            pass
            # self.app.add_url_rule("/shutdown", "get", shutdown, methods=["GET"])

        thread_name = f'{self.name}_thread'
        flask_srv = Process(target=self.app.run, kwargs={"host": "0.0.0.0", "port": self.port}, name=thread_name)
        flask_srv.daemon = False
        flask_srv.start()
        self.log.info(f'{self.name} Flask lives on PID: {flask_srv.pid}')
        ## app.is_ready() would be good here, rather than horrible polling inside a try
        tries=0

        from requests import get

        while True:
            if tries>20:
                self.log.error(f'Cannot ping the {self.name}!')
                self.log.error('This can happen if the web proxy is on at NP04.'+
                               '\nExit NanoRC and try again after executing:'+
                               '\nsource ~np04daq/bin/web_proxy.sh -u')
                raise RuntimeError(f"Cannot create a {self.name}")
            tries += 1
            try:
                resp = get(f"http://0.0.0.0:{self.port}/readystatus")
                if resp.text == "ready":
                    break
            except Exception as e:
                pass
            time.sleep(0.5)

        # We don't release that lock before we have received a "ready" from the listener
        with self.ready_lock:
            self.ready = True

        return flask_srv

    def stop(self) -> NoReturn:
        self.flask.terminate()
        self.flask.join()
        self.join()

    def is_ready(self):
        with self.ready_lock:
            return self.ready

    def _create_and_join_flask(self):
        with self.ready_lock:
            self.ready = False

        self.flask = self._create_flask()
        self.flask.join()
        with self.ready_lock:
            self.ready = False

        self.log.info(f'{self.name}-flaskmanager terminated')

    def run(self) -> NoReturn:
        self._create_and_join_flask()


