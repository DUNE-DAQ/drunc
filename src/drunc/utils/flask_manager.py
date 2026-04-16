"""Flask application manager utilities for DRUNC."""

import os
import signal
import threading
import time
from multiprocessing import Process
from typing import TYPE_CHECKING, Protocol

import psutil
import requests
from flask import Flask, jsonify, make_response, request

if TYPE_CHECKING:

    class _GunicornConfig(Protocol):
        settings: dict[str, object]

        def set(self, key: str, value: object) -> None: ...

    class _BaseApplication:
        cfg: _GunicornConfig

        def __init__(self, *args: object, **kwargs: object) -> None: ...

        def run(self) -> None: ...

    class _Resource:
        pass

    class Api:
        """Typing stub for flask_restful.Api."""

        def __init__(self, app: Flask) -> None:
            """Initialize the API with a Flask application."""
            ...

        def add_resource(
            self, resource: type[_Resource], *urls: str, **kwargs: object
        ) -> None:
            """Register a resource class on one or more URL routes."""
            ...

else:
    from flask_restful import Api
    from flask_restful import Resource as _Resource
    from gunicorn.app.base import BaseApplication as _BaseApplication

from drunc.exceptions import DruncCommandException
from drunc.utils.utils import get_logger, get_new_port


class GunicornStandaloneApplication(_BaseApplication):
    """Standalone Gunicorn application wrapper."""

    def __init__(
        self,
        app: Flask,
        options: dict[str, object] | None = None,
    ) -> None:
        """Initialize a GunicornStandaloneApplication.

        Args:
            app: The Flask application to run.
            options: Configuration options for Gunicorn. Defaults to None.
        """
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self) -> None:
        """Load Gunicorn configuration from options."""
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self) -> Flask:
        """Load the Flask application.

        Returns:
            Flask: The Flask application.
        """
        return self.application


class CannotStartFlaskManager(DruncCommandException):
    """Exception raised when the Flask manager cannot start."""

    pass


class FlaskManager(threading.Thread):
    """Manager for Flask applications running in a separate thread.

    It allows to have a Flask server under a thread,
    start and stop it. Note that it creates another endpoint accessible at the route
    /readystatus. This is used to poll if the service is up, however the user can
    provide it.

    To use this code, one can use the following example:

    ```python
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
    ```

    Then, later on, to stop it:

    ```python
    manager.stop()
    ```
    """

    def __init__(
        self,
        name: str,
        app: Flask,
        port: int,
        workers: int = 1,
        host: str = "0.0.0.0",
    ) -> None:
        """Initialize a FlaskManager.

        Args:
            name: The name of the Flask manager.
            app: The Flask application to manage.
            port: The port to run the Flask server on.
            workers: The number of Gunicorn workers. Defaults to 1.
            host: The host address to bind to. Defaults to "0.0.0.0".
        """
        super(FlaskManager, self).__init__(daemon=True)
        self.log = get_logger(f"{name}-flaskmanager", stream_handlers=True)
        self.name = name
        self.app = app
        self.prod_app: GunicornStandaloneApplication | None = None
        self.flask: Process | None = None

        self.host = host
        self.port = port

        self.workers = workers
        self.gunicorn_pid: int | None = None
        self.ready = False
        self.joined = False
        self.ready_lock = threading.Lock()

    def _create_flask(self) -> Process:
        need_ready = True
        for rule in self.app.url_map.iter_rules():
            if "get_ready_status" in rule.endpoint:
                need_ready = False

        def get_ready_status() -> str:
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
        prod_app = self.prod_app
        assert prod_app is not None, "GunicornStandaloneApplication creation failed"

        def run_gunicorn_with_signal_handling() -> None:
            """Run gunicorn with SIGHUP ignored to prevent reload on shutdown.

            This prevents gunicorn from reloading when the parent process receives SIGHUP.
            We only want graceful shutdown via SIGTERM from FlaskManager.stop().
            """
            # Create new process group first to isolate from parent's signal propagation
            # This prevents SIGHUP from being sent to this process when parent receives it
            try:
                os.setpgid(0, 0)  # Create new process group (safer than setsid)
            except (OSError, PermissionError):
                # May fail if already in a process group or on some systems, ignore
                pass

            prod_app.run()

        thread_name = f"{self.name}_thread"
        flask_srv = Process(  # Indeed, we've just forked this sucker
            target=run_gunicorn_with_signal_handling, name=thread_name, daemon=True
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

    def __del__(self) -> None:
        """Cleanup when the FlaskManager is destroyed."""
        self.stop()

    def stop(self) -> None:
        """Stop the Flask manager and terminate the Gunicorn process.

        Sends SIGTERM to the Gunicorn process and joins the Flask process thread.
        """
        if self.gunicorn_pid:
            gunicorn_proc = psutil.Process(self.gunicorn_pid)
            # https://github.com/benoitc/gunicorn/blob/ab9c8301cb9ae573ba597154ddeea16f0326fc15/docs/source/signals.rst#master-process
            # TOTAL DESTRUCTION
            gunicorn_proc.send_signal(signal.SIGTERM)
            if self.flask is not None:
                self.flask.terminate()

        self.join()

    def restart_renew(self) -> "FlaskManager":
        """Restart and renew the Flask manager.

        Stops the current instance and creates a new one with the same configuration.

        Returns:
            FlaskManager: A new FlaskManager instance with the same settings.
        """
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

    def is_ready(self) -> bool:
        """Check if the Flask manager is ready to serve requests.

        Returns:
            bool: True if ready, False otherwise.
        """
        with self.ready_lock:
            return self.ready

    def is_terminated(self) -> bool:
        """Check if the Flask manager has been terminated.

        Returns:
            bool: True if terminated, False otherwise.
        """
        with self.ready_lock:
            return self.joined

    def _create_and_join_flask(self) -> None:
        with self.ready_lock:
            self.ready = False
            self.joined = False

        self.flask = self._create_flask()
        self.flask.join()
        with self.ready_lock:
            self.ready = False
            self.joined = True

        self.log.info(f"{self.name}-flaskmanager terminated")

    def run(self) -> None:
        """Run the Flask server in the thread.

        This method is called when the thread is started.
        """
        self._create_and_join_flask()


def main() -> None:
    """Main entry point for demonstrating the FlaskManager.

    Creates a simple Flask application with a dummy endpoint and starts it.
    """

    class DummyEndpoint(_Resource):
        def post(self) -> None:
            print(request)

        def get(self) -> object:
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
