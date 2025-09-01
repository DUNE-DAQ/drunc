import getpass
import json
import os
import signal
import tempfile
import time
from collections.abc import Iterator
from time import sleep

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
from drunc.utils.shell_utils import GRPCDriver
from drunc.utils.utils import (
    get_control_type_and_uri_from_connectivity_service,
    get_logger,
    host_is_local,
    resolve_localhost_and_127_ip_to_network_ip,
    resolve_localhost_to_hostname,
)


class ProcessManagerDriver(GRPCDriver):
    controller_address = ""

    def __init__(self, address: str, token: Token):
        self.log = get_logger("controller.ProcessManagerDriver")
        self.address = address
        self.channel = grpc.insecure_channel(self.address)
        self.stub = ProcessManagerStub(self.channel)
        self.token = copy_token(token)

    def _convert_oks_to_boot_request(
        self,
        oks_conf: str,
        user: str,
        session_dal,
        db,
        session_name: str,
        override_logs: bool,
    ) -> Iterator[BootRequest]:
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

        pwd = os.getcwd()

        session_log_path = session_dal.log_path
        if session_log_path == "./":
            session_log_path = pwd

        for app in apps:
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
            executable_and_arguments = []

            if session_dal.rte_script:
                executable_and_arguments.append(
                    ProcessDescription.ExecAndArgs(
                        exec="source", args=[session_dal.rte_script]
                    )
                )

            else:
                rte_script = get_rte_script()
                if not rte_script:
                    raise DruncSetupException("No RTE script found.")

                executable_and_arguments.append(
                    ProcessDescription.ExecAndArgs(exec="source", args=[rte_script])
                )

            executable_and_arguments.append(
                ProcessDescription.ExecAndArgs(exec=exe, args=args)
            )
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
            yield breq

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
    ) -> Iterator[ProcessInstanceList]:
        from daqconf.consolidate import consolidate_db

        self.log.info(f"Booting session [green]{session_name}[/green]")

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

        import conffwk  # isort: skip

        db = conffwk.Configuration(conf_file)
        session_dal = db.get_dal(class_name="Session", uid=conf_id)

        csc = None
        if session_dal.connectivity_service:
            connection_server = session_dal.connectivity_service.host
            connection_port = session_dal.connectivity_service.service.port
            csc = ConnectivityServiceClient(
                session_name, f"{connection_server}:{connection_port}"
            )

        last_boot_on_host_at = {}
        previous_host = None

        for request in self._convert_oks_to_boot_request(
            oks_conf=conf_file,
            user=user,
            session_dal=session_dal,
            session_name=session_name,
            db=db,
            override_logs=override_logs,
            **kwargs,
        ):
            if (
                request.process_description.metadata.name
                not in [app.id for app in session_dal.infrastructure_applications]
                and csc
                and not csc.is_ready(timeout=10)
            ):
                raise DruncSetupException("Connectivity service is not ready in time")

            this_host = next(iter(request.process_restriction.allowed_hosts))

            time_diff = time.time() - last_boot_on_host_at.get(this_host, 0)

            if sleep_between_app_boot > 0 and time_diff < sleep_between_app_boot:
                self.log.debug(
                    f"Sleeping for {sleep_between_app_boot - time_diff} seconds {previous_host} {this_host}"
                )
                sleep(sleep_between_app_boot - time_diff)

            previous_host = this_host
            last_boot_on_host_at[this_host] = time.time()

            try:
                response = self.stub.boot(request, timeout=timeout)
            except grpc.RpcError as e:
                handle_grpc_error(e)

            yield response

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
                    self.log.error(
                        f"""
Could not find \'{top_controller_name}\' on the connectivity service.

Two possibilities:

1. The most likely, the controller died. You can check that by looking for error like:
[yellow]Process \'{top_controller_name}\' (session: \'{session_name}\', user: \'{getpass.getuser()}\') process exited with exit code 1).[/]
Try running [yellow]ps[/] to see if the {top_controller_name} is still running.
You may also want to check the logs of the controller, try typing:
[yellow]logs --name {top_controller_name} --how-far 1000[/]
If that's not helping, you can restart this shell with [yellow]--log-level debug[/], and look out for \'STDOUT\' and \'STDERR\'.

2. The controller did not die, but is still setting up and has not advertised itself on the connection service.
You may be able to connect to the {top_controller_name} in a bit. Check the logs of the controller:
[yellow]logs --name {top_controller_name} --grep grpc[/]
And look for messages like:
[yellow]Registering root-controller to the connectivity service at grpc://xxx.xxx.xxx.xxx:xxxxx[/]
To find the controller address, you can look up \'{top_controller_name}_control\' on http://{resolve_localhost_to_hostname(connection_server)}:{connection_port} (you may need a SOCKS proxy from outside CERN), or use the address from the logs as above. Then just connect this shell to the controller with:
[yellow]connect {{controller_address}}:{{controller_port}}>[/]
"""
                    )
                    return

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
                self.log.warning(
                    f"""This shell didn't connect to the {top_controller_name}.
To find the controller address, you can look up \'{top_controller_name}_control\' on http://{resolve_localhost_to_hostname(connection_server)}:{connection_port} (you may need a SOCKS proxy from outside CERN), or use the address from the logs as above. Then just connect this shell to the controller with:
[yellow]connect {{controller_address}}:{{controller_port}}>[/]
"""
                )
            else:
                self.log.warning(
                    f"This shell didn't connect to the {top_controller_name}. You can use the connect command to connect to the controller."
                )
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)

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
        executable_and_arguments = [
            ProcessDescription.ExecAndArgs(exec="echo", args=["Starting dummy_boot."])
        ]
        for i in range(1, n_sleeps + 1):
            executable_and_arguments += [
                ProcessDescription.ExecAndArgs(exec="sleep", args=[str(sleep) + "s"]),
                ProcessDescription.ExecAndArgs(
                    exec="echo", args=[str(sleep * i) + "s"]
                ),
            ]
        executable_and_arguments.append(
            ProcessDescription.ExecAndArgs(exec="echo", args=["Exiting."])
        )

        for process in range(n_processes):
            request = BootRequest(
                token=copy_token(self.token),
                process_description=ProcessDescription(
                    metadata=ProcessMetadata(
                        user=user,
                        session=session_name,
                        name="dummy_boot_" + str(process),
                        hostname="",
                    ),
                    executable_and_arguments=executable_and_arguments,
                    env={},
                    process_execution_directory=pwd,
                    process_logs_path=f"{pwd}/log_{user}_{session_name}_dummy-boot_"
                    + str(process)
                    + ".log",
                ),
                process_restriction=ProcessRestriction(allowed_hosts=["localhost"]),
            )
            self.log.debug(f"{request=}\n\n")

            try:
                response = self.stub.boot(request, timeout=timeout)
            except grpc.RpcError as e:
                handle_grpc_error(e)

            yield response

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
