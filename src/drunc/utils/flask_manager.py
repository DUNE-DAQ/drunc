import signal
import threading
import time
from multiprocessing import Process
from typing import NoReturn

import gunicorn.app.base
import psutil
import requests
from flask import Flask, jsonify, make_response, request
from flask_restful import Api, Resource

from drunc.exceptions import DruncCommandException
from drunc.utils.utils import get_logger, get_new_port


class GunicornStandaloneApplication(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


class CannotStartFlaskManager(DruncCommandException):
    pass


class FlaskManager(threading.Thread):
    """This class is a manager for flask.
    It allows to have a Flask server under a thread, start and stop it.
    Note that it creates another -trivial- endpoint accessible at the route /readystatus.
    This is used to poll if the service is up, however the user can provide it, and

    To use this code, one can use the following example:

    <snippet>
    from flask import Flask
    from flask_restful import Api
    app = Flask('some-name')
    api = Api(app)
    api.add_resource(
        AnEndpointResourceClass, "/endpoint",
    )

    from flask_manager import FlaskManager
    manager = FlaskManager(
        port = port,
        app = app,
        name = "some-name"
    )

    manager.start()
    while not manager.is_ready():
        from time import sleep
        sleep(0.1)
    </snippet>

    Then, later on, to stop it:
    <snippet>
    manager.stop()
    </snippet>
    """

    def __init__(self, name, app, port, workers=1, host="0.0.0.0"):
        super(FlaskManager, self).__init__(daemon=True)
        self.log = get_logger(f"utils.{name}-flaskmanager")
        self.name = name
        self.app = app
        self.prod_app = None
        self.flask = None

        self.host = host
        self.port = port

        self.workers = workers
        self.gunicorn_pid = None
        self.ready = False
        self.joined = False
        self.ready_lock = threading.Lock()

    def _create_flask(self) -> Process:
        need_ready = True
        for rule in self.app.url_map.iter_rules():
            if "get_ready_status" in rule.endpoint:
                need_ready = False

        def get_ready_status():
            return "ready"

        if need_ready:
            self.app.add_url_rule(
                "/readystatus", "get_ready_status", get_ready_status, methods=["GET"]
            )

        self.prod_app = GunicornStandaloneApplication(
            app=self.app,
            options={
                "bind": f"{self.host}:{self.port}",
                "workers": self.workers,
            },
        )

        thread_name = f"{self.name}_thread"
        flask_srv = Process(  # Indeed, we've just forked this sucker
            target=self.prod_app.run, name=thread_name, daemon=True
        )
        flask_srv.start()

        self.gunicorn_pid = None

        for _ in range(10):
            if flask_srv.is_alive():
                self.gunicorn_pid = flask_srv.pid
                break
            time.sleep(0.5)

        if self.gunicorn_pid is None:
            raise CannotStartFlaskManager(
                f"Cannot start a FlaskManager for {self.name}"
            )

        tries = 0
        stored_exception = None

        while True:
            if tries > 20:
                self.log.critical(f"Cannot ping the {self.name}!")
                self.log.critical(
                    "This can happen if the web proxy is on at NP04."
                    + "\nExit drunc and try again after executing:"
                    + "\nsource ~np04daq/bin/web_proxy.sh -u"
                )

                if not flask_srv.is_alive():
                    self.log.critical(
                        f"{self.name} is not alive, it exited with code {flask_srv.exitcode}"
                    )

                raise CannotStartFlaskManager(
                    f"Cannot start a FlaskManager for {self.name}"
                ) from stored_exception
            tries += 1
            try:
                resp = requests.get(f"http://{self.host}:{self.port}/readystatus")
                if resp.text == "ready":
                    break
            except Exception as e:
                stored_exception = e

            time.sleep(0.5)

        self.log.info(f"{self.name} is ready")
        # We don't release that lock before we have received a "ready" from the listener
        with self.ready_lock:
            self.ready = True

        return flask_srv

    def __del__(self):
        self.stop()

    def stop(self) -> NoReturn:
        # gunicorn is forked, so we need to now need send signal ourselves
        if self.gunicorn_pid:
            gunicorn_proc = psutil.Process(self.gunicorn_pid)
            # https://github.com/benoitc/gunicorn/blob/ab9c8301cb9ae573ba597154ddeea16f0326fc15/docs/source/signals.rst#master-process
            # TOTAL DESTRUCTION
            gunicorn_proc.send_signal(signal.SIGTERM)
            self.flask.terminate()

        self.join()

    def restart_renew(self):
        # well, we cannot really do that.
        # we have to hack it a bit:
        # unfortunately, this means you need to do:
        # manager = manager.restart_renew()

        fm = FlaskManager(
            name=self.name,
            app=self.app,
            port=self.port,
            workers=self.workers,
            host=self.host,
        )
        fm.start()
        while not fm.is_ready():
            time.sleep(0.1)
        return fm

    def is_ready(self):
        with self.ready_lock:
            return self.ready

    def is_terminated(self):
        with self.ready_lock:
            return self.joined

    def _create_and_join_flask(self):
        with self.ready_lock:
            self.ready = False
            self.joined = False

        self.flask = self._create_flask()
        self.flask.join()
        with self.ready_lock:
            self.ready = False
            self.joined = True

        self.log.info(f"{self.name}-flaskmanager terminated")

    def run(self) -> NoReturn:
        self._create_and_join_flask()


def main():
    class DummyEndpoint(Resource):
        def post(self):
            print(request)

        def get(self):
            return make_response(jsonify({"weeeee": "wooo"}))

    app = Flask("test-app")
    api = Api(app)
    api.add_resource(DummyEndpoint, "/dummy", methods=["GET", "POST"])

    for _ in range(10):
        try:
            manager = FlaskManager(
                port=get_new_port(), app=app, name="test_name", host="127.0.0.1"
            )
        except:
            continue
        else:
            manager.start()
            while not manager.is_ready():
                time.sleep(0.1)
            assert not manager.is_terminated()
            assert manager.is_ready()

            requests.get(f"http://127.0.0.1:{manager.port}/dummy")
            print("succesfully got endpoint /dummy")
            manager.stop()
            assert manager.is_terminated()
            assert not manager.is_ready()

            manager = manager.restart_renew()
            assert not manager.is_terminated()
            assert manager.is_ready()
            requests.get(f"http://127.0.0.1:{manager.port}/dummy")
            print("succesfully got endpoint /dummy")
            manager.stop()
            assert manager.is_terminated()
            assert not manager.is_ready()
            break


if __name__ == "__main__":
    main()
