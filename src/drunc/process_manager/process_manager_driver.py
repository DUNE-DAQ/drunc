import getpass
import json
import os
import signal
import tempfile
import time
from collections.abc import Iterator
from time import sleep
from typing import Any

import grpc
from druncschema.description_pb2 import Description
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
)
from druncschema.process_manager_pb2_grpc import ProcessManagerStub
from druncschema.request_response_pb2 import Request
from druncschema.token_pb2 import Token

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.controller.utils import get_segment_lookup_timeout
from drunc.exceptions import DruncSetupException, DruncShellException
from drunc.process_manager.utils import get_log_path, get_rte_script
from drunc.utils.grpc_utils import copy_token, handle_grpc_error
from drunc.utils.utils import (
    get_control_type_and_uri_from_connectivity_service,
    get_logger,
    host_is_local,
    resolve_localhost_and_127_ip_to_network_ip,
    resolve_localhost_to_hostname,
)


class ProcessManagerDriver:
    controller_address = ""

    def __init__(self, address: str, token: Token):
        self.log = get_logger("controller.ProcessManagerDriver")
        self.address = address
        options = [
            ("grpc.keepalive_time_ms", 60000)  # pings the server every 60 seconds
        ]
        self.channel = grpc.insecure_channel(self.address, options=options)
        self.stub = ProcessManagerStub(self.channel)
        self.token = copy_token(token)

    # ----- Boot workflow -----
    def boot(
        self,
        conf_file: str,
        conf_id: str,
        user: str,
        session_name: str,
        log_level: str,
        override_logs: bool = True,
        timeout: int | float = 60,
        sleep_between_app_boot: (
            int | float
        ) = 0,  # This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.
        **kwargs,
    ) -> Iterator[ProcessInstanceList] | None:
        self.log.info(f"Booting session [green]{session_name}[/green]")

        # Step 1 - consolidate configuration
        self._consolidate_config(session_name, conf_file)

        # Step 2 - initialise session
        db, session_dal = self._initialise_session(conf_file, conf_id)

        # Step 3 - connect_to_service
        csc, connection_server, connection_port = self._connect_to_service(
            session_dal, session_name
        )

        # Step 4 - track boot timings per host
        last_boot_on_host_at = {}
        previous_host = None

        # Step 5: iterate over boot requests
        for request in self._convert_oks_to_boot_request(
            oks_conf=conf_file,
            user=user,
            session_dal=session_dal,
            session_name=session_name,
            db=db,
            override_logs=override_logs,
            **kwargs,
        ):
            if not request:
                self.log.error("[red]No boot request was generated, ending boot.[/red]")
                return None
            if request.process_description.metadata.name in [
                app.id for app in session_dal.infrastructure_applications
            ]:
                self.log.debug(
                    f"Skipping connectivity service readiness check for application {request.process_description.metadata.name}"
                )
            else:
                self.log.debug(
                    f"Checking connectivity service readiness before booting application {request.process_description.metadata.name}"
                )
                if csc and not csc.is_ready(timeout=10):
                    raise DruncSetupException(
                        "Connectivity service did not respond within timeout."
                    )

            this_host = next(iter(request.process_restriction.allowed_hosts))

            time_diff = time.time() - last_boot_on_host_at.get(this_host, 0)

            if sleep_between_app_boot > 0 and time_diff < sleep_between_app_boot:
                self.log.debug(
                    f"Sleeping for {sleep_between_app_boot - time_diff} seconds {previous_host} {this_host}"
                )
                sleep(sleep_between_app_boot - time_diff)

            previous_host = this_host
            last_boot_on_host_at[this_host] = time.time()
            self.log.debug(f"Boot request: {request}")
            try:
                response = self.stub.boot(request, timeout=timeout)
            except grpc.RpcError as e:
                handle_grpc_error(e)
            yield response

        # Step 6: discover controller
        self._discover_controller(
            session_dal, session_name, csc, connection_server, connection_port
        )

    def _collect_all_apps(
        self,
        oks_conf: str,
        session_dal,
        db,
        session_name: str,
    ) -> list[dict]:
        from drunc.process_manager.oks_parser import collect_apps, collect_infra_apps

        env = {
            "DUNEDAQ_SESSION": session_name,
        }

        apps = collect_apps(
            session_name=session_name,
            config_filename=oks_conf,
            db=db,
            session_obj=session_dal,
            segment_obj=session_dal.segment,
            env=env,
            tree_prefix=[
                0,
            ],
        )

        # Next line gets the max of all the first number in the tree id, and adds 1 to it.
        next_tree_id = max([int(app["tree_id"].split(".")[0]) for app in apps]) + 1
        infra_apps = collect_infra_apps(session_dal, env, tree_prefix=[next_tree_id])

        apps = infra_apps + apps

        self.log.debug(f"{json.dumps(apps, indent=4)}")

        return apps

    def _prepare_exec_and_args(
        self, session_dal, exe: str, args: list[str]
    ) -> list[ProcessDescription.ExecAndArgs]:
        """
        Prepare
        """

        executable_and_arguments = []

        if session_dal.rte_script:
            executable_and_arguments.append(
                ProcessDescription.ExecAndArgs(
                    exec="source", args=[session_dal.rte_script]
                )
            )

        else:
            try:
                rte_script = get_rte_script()
            except DruncSetupException as e:
                log = get_logger("utils.check_rte")
                errmsg = f"[red]Couldn't understand where to find the rte script [/red]. Did you run [green] dbt-build [/green] and [green]dbt-workarea-env[/green]?. {e}"
                log.error(errmsg)
                raise

            executable_and_arguments.append(
                ProcessDescription.ExecAndArgs(exec="source", args=[rte_script])
            )

        executable_and_arguments.append(
            ProcessDescription.ExecAndArgs(exec=exe, args=args)
        )
        return executable_and_arguments

    def _build_boot_request(
        self,
        app: dict,
        user: str,
        session_name: str,
        session_dal,
        session_log_path: str,
        override_logs: bool,
        pwd: str,
    ) -> BootRequest:
        host = app["restriction"]
        name = app["name"]
        exe = app["type"]
        args = app["args"]
        env = app["env"]
        app_log_path = app["log_path"]
        env["DUNE_DAQ_BASE_RELEASE"] = os.getenv("DUNE_DAQ_BASE_RELEASE")
        env["SPACK_RELEASES_DIR"] = os.getenv("SPACK_RELEASES_DIR")
        tree_id = app["tree_id"]
        self.log.debug(f"{name}:\n{json.dumps(app, indent=4)}")

        try:
            executable_and_arguments = self._prepare_exec_and_args(
                session_dal, exe, args
            )
        except DruncSetupException:
            raise DruncSetupException("Generating executable and arguments failed")

        log_path = get_log_path(
            user=user,
            session_name=session_name,
            application_name=name,
            override_logs=override_logs,
            app_log_path=app_log_path,
            session_log_path=session_log_path,
        )

        if host_is_local(host) and not os.path.exists(os.path.dirname(log_path)):
            raise DruncShellException(f"Log path {log_path} does not exist.")

        self.log.debug(f"{name}'s env:\n{env}")
        breq = BootRequest(
            token=copy_token(self.token),
            process_description=ProcessDescription(
                metadata=ProcessMetadata(
                    user=user,
                    session=session_name,
                    name=name,
                    hostname="",
                    tree_id=tree_id,
                ),
                executable_and_arguments=executable_and_arguments,
                env=env,
                process_execution_directory=pwd,
                process_logs_path=log_path,
            ),
            process_restriction=ProcessRestriction(allowed_hosts=[host]),
        )
        self.log.debug(f"{breq=}\n\n")
        return breq

    def _convert_oks_to_boot_request(
        self,
        oks_conf: str,
        user: str,
        session_dal,
        db,
        session_name: str,
        override_logs: bool,
    ) -> Iterator[BootRequest]:
        apps = self._collect_all_apps(oks_conf, session_dal, db, session_name)

        pwd = os.getcwd()

        session_log_path = session_dal.log_path
        if session_log_path == "./":
            session_log_path = pwd

        for app in apps:
            try:
                breq = self._build_boot_request(
                    app,
                    user,
                    session_name,
                    session_dal,
                    session_log_path,
                    override_logs,
                    pwd,
                )
            except DruncSetupException as e:
                log = get_logger("utils.boot_req_generator")
                log.error(f"[red]Caught exception in boot generator [/red]: {e}")
                yield None
            yield breq

    def _consolidate_config(self, session_name, conf_file: str) -> str | None:
        from daqconf.consolidate import consolidate_db

        self.log.debug(f"Validating {session_name} configuration")

        with tempfile.NamedTemporaryFile(suffix=".data.xml", delete=True) as f:
            f.flush()
            f.seek(0)
            fname = f.name
            try:
                conf_file_no_scheme = conf_file.replace("oksconflibs:", "")
                consolidate_db(conf_file_no_scheme, f"{fname}")
            except Exception as e:
                self.log.critical(
                    f"""\nInvalid configuration passed (cannot consolidate your configuration)
{e}
To debug it, close drunc and run the following command:

[yellow]oks_dump --files-only {conf_file_no_scheme}[/]

"""
                )
                return

    def _initialise_session(self, conf_file: str, conf_id: str) -> tuple:
        import conffwk  # isort: skip

        db = conffwk.Configuration(conf_file)
        session_dal = db.get_dal(class_name="Session", uid=conf_id)
        return db, session_dal

    def _connect_to_service(
        self, session_dal, session_name: str
    ) -> ConnectivityServiceClient | None:
        if session_dal.connectivity_service:
            connection_server = session_dal.connectivity_service.host
            connection_port = session_dal.connectivity_service.service.port
            client = ConnectivityServiceClient(
                session_name, f"{connection_server}:{connection_port}"
            )
            return client, connection_server, connection_port
        return None, None, None

    def _discover_controller(
        self,
        session_dal: Any,
        session_name: str,
        csc: ConnectivityServiceClient | None,
        connection_server: str,
        connection_port: int,
    ):
        """
        Attempts to discover the controller address after booting applications.
        """
        top_controller_name = session_dal.segment.controller.id

        def get_controller_address(session_dal, session_name):
            from drunc.process_manager.oks_parser import collect_variables

            env = {}
            collect_variables(session_dal.environment, env)
            if csc:
                try:
                    timeout = (
                        get_segment_lookup_timeout(session_dal.segment, 60) + 60
                    )  # root-controller timout to find all its children + 60s for the root controller to start itself
                    self.log.debug(
                        f"Using a timeout of {timeout}s to find the [green]{top_controller_name}[/] on the connectivity service"
                    )
                    _, uri = get_control_type_and_uri_from_connectivity_service(
                        csc,
                        name=top_controller_name,
                        timeout=timeout,
                        retry_wait=1,
                        progress_bar=True,
                        title=f"Looking for [green]{top_controller_name}[/] on the connectivity service...",
                    )
                except ApplicationLookupUnsuccessful:
                    self._log_controller_lookup_failure(
                        session_name,
                        top_controller_name,
                        connection_server,
                        connection_port,
                    )
                    return None

                return uri.replace("grpc://", "")

            service_id = top_controller_name + "_control"
            port_number = None
            protocol = None

            for service in session_dal.segment.controller.exposes_service:
                if service.id == service_id:
                    port_number = service.port
                    protocol = service.protocol
                    break
            if port_number is None or protocol is None:
                return None

            ip = resolve_localhost_and_127_ip_to_network_ip(
                session_dal.segment.controller.runs_on.runs_on.id
            )
            return f"{ip}:{port_number}"

        def keyboard_interrupt_on_sigint(signal, frame):
            self.log.warning("Interrupted")
            raise KeyboardInterrupt

        original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, keyboard_interrupt_on_sigint)
        try:
            self.controller_address = get_controller_address(session_dal, session_name)
        except KeyboardInterrupt:
            if session_dal.connectivity_service:
                connection_server = session_dal.connectivity_service.host
                connection_port = session_dal.connectivity_service.service.port
                self._log_controller_interrupt(
                    self, top_controller_name, connection_server, connection_port
                )
            else:
                self.log.warning(
                    f"This shell didn't connect to the {top_controller_name}. You can use the connect command to connect to the controller."
                )
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)

    # ----- Dummy boot workflow -----
    def dummy_boot(
        self,
        user: str,
        session_name: str,
        n_processes: int,
        sleep: int,
        n_sleeps: int,
        timeout: int | float = 60,
    ) -> Iterator[ProcessInstanceList]:
        pwd = os.getcwd()

        # Construct the list of commands to send to the dummy_boot process
        executable_and_arguments = self._prepare_exec_and_args_dummy_boot(
            sleep, n_sleeps
        )

        for process in range(n_processes):
            request = self._build_boot_request_dummy_boot(
                user=user,
                session_name=session_name,
                process=process,
                exec_args=executable_and_arguments,
                pwd=pwd,
            )
            self.log.debug(f"{request=}\n\n")

            try:
                response = self.stub.boot(request, timeout=timeout)
            except grpc.RpcError as e:
                handle_grpc_error(e)

            yield response

    def _prepare_exec_and_args_dummy_boot(self, sleep: int, n_sleeps: int) -> list:
        args = [
            ProcessDescription.ExecAndArgs(exec="echo", args=["Starting dummy_boot."])
        ]
        for i in range(1, n_sleeps + 1):
            args.extend(
                [
                    ProcessDescription.ExecAndArgs(exec="sleep", args=[f"{sleep}s"]),
                    ProcessDescription.ExecAndArgs(exec="echo", args=[f"{sleep * i}s"]),
                ]
            )
        args.append(ProcessDescription.ExecAndArgs(exec="echo", args=["Exiting."]))
        return args

    def _build_boot_request_dummy_boot(
        self, user: str, session_name: str, process: int, exec_args: list, pwd: str
    ) -> BootRequest:
        return BootRequest(
            token=copy_token(self.token),
            process_description=ProcessDescription(
                metadata=ProcessMetadata(
                    user=user,
                    session=session_name,
                    name=f"dummy_boot_{process}",
                    hostname="",
                ),
                executable_and_arguments=exec_args,
                env={},
                process_execution_directory=pwd,
                process_logs_path=f"{pwd}/log_{user}_{session_name}_dummy-boot_{process}.log",
            ),
            process_restriction=ProcessRestriction(allowed_hosts=["localhost"]),
        )

    # ----- RPC methods -----
    def terminate(
        self,
        timeout: int | float = 60,
    ) -> ProcessInstanceList:
        request = Request(token=copy_token(self.token))

        try:
            response = self.stub.terminate(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def kill(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.kill(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def logs(self, request: LogRequest, timeout: int | float = 60) -> LogLines:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.logs(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def ps(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.ps(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def flush(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.flush(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def restart(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.restart(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def describe(self, timeout: int | float = 60) -> Description:
        request = Request(token=copy_token(self.token))

        try:
            response = self.stub.describe(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    # ----- logging helpers -----

    def _log_controller_lookup_failure(
        self, session_name, top_controller_name, connection_server, connection_port
    ):
        # Logs detailed troubleshooting steps
        self.log.error(
            f"""
# Could not find \'{top_controller_name}\' on the connectivity service.

# Two possibilities:

# 1. The most likely, the controller died. You can check that by looking for error like:
# [yellow]Process \'{top_controller_name}\' (session: \'{session_name}\', user: \'{getpass.getuser()}\') process exited with exit code 1).[/]
# Try running [yellow]ps[/] to see if the {top_controller_name} is still running.
# You may also want to check the logs of the controller, try typing:
# [yellow]logs --name {top_controller_name} --how-far 1000[/]
# If that's not helping, you can restart this shell with [yellow]--log-level debug[/], and look out for \'STDOUT\' and \'STDERR\'.

# 2. The controller did not die, but is still setting up and has not advertised itself on the connection service.
# You may be able to connect to the {top_controller_name} in a bit. Check the logs of the controller:
# [yellow]logs --name {top_controller_name} --grep grpc[/]
# And look for messages like:
# [yellow]Registering root-controller to the connectivity service at grpc://xxx.xxx.xxx.xxx:xxxxx[/]
# To find the controller address, you can look up \'{top_controller_name}_control\' on http://{resolve_localhost_to_hostname(connection_server)}:{connection_port} (you may need a SOCKS proxy from outside CERN), or use the address from the logs as above. Then just connect this shell to the controller with:
# [yellow]connect {{controller_address}}:{{controller_port}}>[/]
            """
        )

    def _log_controller_interrupt(
        self, top_controller_name, connection_server, connection_port
    ):
        # Logs recovery instructions after user interrupts controller lookup
        self.log.warning(
            f"""This shell didn't connect to the {top_controller_name}.
To find the controller address, you can look up \'{top_controller_name}_control\' on http://{resolve_localhost_to_hostname(connection_server)}:{connection_port} (you may need a SOCKS proxy from outside CERN), or use the address from the logs as above. Then just connect this shell to the controller with:
[yellow]connect {{controller_address}}:{{controller_port}}>[/]
"""
        )
