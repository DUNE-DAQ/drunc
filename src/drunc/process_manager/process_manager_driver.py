import getpass
import json
import os
import signal
import tempfile
import time
from collections.abc import Iterator
from time import sleep
from typing import Any, Dict, List

import conffwk
import grpc
from daqconf.set_connectivity_service_port import set_connectivity_service_port
from daqconf.set_rc_controller_port import set_rc_controller_port
from daqconf.utils import find_free_port
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
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.token_pb2 import Token

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.controller.utils import get_segment_lookup_timeout
from drunc.exceptions import DruncSetupException, DruncShellException
from drunc.process_manager.oks_parser import get_full_db_path
from drunc.process_manager.utils import (
    get_log_path,
    get_rte_script,
)
from drunc.utils.grpc_utils import (
    copy_token,
    extract_grpc_rich_error,
    handle_grpc_error,
)
from drunc.utils.utils import (
    file_is_read_only,
    get_control_type_and_uri_from_connectivity_service,
    get_logger,
    host_is_local,
    is_port_available,
    resolve_localhost_and_127_ip_to_network_ip,
    resolve_localhost_to_hostname,
    strip_non_drunc_loggers,
)


class ProcessManagerDriver:
    controller_address = ""

    def __init__(self, address: str, token: Token):
        self.log = get_logger("process_manager_driver", rich_handler=True)
        self.address = address
        options = [
            ("grpc.keepalive_time_ms", 60000)  # pings the server every 60 seconds
        ]
        self.channel = grpc.insecure_channel(self.address, options=options)
        self.stub = ProcessManagerStub(self.channel)
        self.token = copy_token(token)

    def close(self) -> None:
        """
        Close the gRPC channel.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        try:
            self.log.debug("Closing gRPC channel to Process Manager")
            self.channel.close()
        except Exception as e:
            self.log.error(f"Error closing gRPC channel: {e}", exc_info=True)

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

        # Step 3 - check for port conflicts and update configuration/DAL as needed
        db, session_dal = self.check_port_conflicts(db, session_dal)

        # Step 4 - connect to the connection service
        csc, connection_server, connection_port = self._connect_to_service(
            session_dal, session_name
        )

        # Step 5 - track boot timings per host
        last_boot_on_host_at = {}
        previous_host = None

        # Step 6: iterate over boot requests
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

            try:
                response = self.stub.boot(request, timeout=timeout)
                yield response

            except grpc.RpcError as e:
                try:
                    error_details = extract_grpc_rich_error(e)
                    self.log.error(error_details)
                except Exception as extraction_error:
                    self.log.debug(
                        f"Could not extract rich error details from gRPC error: {extraction_error}",
                        exc_info=True,
                    )
                handle_grpc_error(e)

        # Step 7: discover segment root controller
        self._discover_controller(
            session_dal, session_name, csc, connection_server, connection_port
        )

    def _collect_all_apps(
        self,
        oks_conf: str,
        session_dal,
        db,
        session_name: str,
    ) -> List[Dict]:
        from drunc.process_manager.oks_parser import collect_apps, collect_infra_apps

        env = {
            "DUNEDAQ_SESSION": session_name,
        }

        apps = collect_apps(
            session_name=session_name,
            config_filename=oks_conf,
            session_obj=session_dal,  # stil the dal for some rason. WHy t
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
        self, session_dal, exe: str, args: List[str]
    ) -> List[ProcessDescription.ExecAndArgs]:
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
        app: Dict,
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
        data_path = app.get("data_path")
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

        process_restriction = ProcessRestriction(allowed_hosts=[host])
        if data_path:
            self.log.debug(
                f"Attaching data_path '{data_path}' to the boot request for '{name}'"
            )
            process_restriction.data_mount = data_path

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
            process_restriction=process_restriction,
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

    def check_port_conflicts(
        self, db: conffwk.Configuration, session_dal: "conffwk.dal.Session"
    ) -> tuple[conffwk.Configuration, "conffwk.dal.Session"]:
        """
        Check that the ports allocated in the configuration file are available. If the
        file is editable, make the changes in the file itself. Otherwise, make the
        changes in the session_dal, logging the difference.

        Note - this logic will go into the run control servuce, where the mapping from
        OKS files will be run.

        Args:
            db - configuration database object to get the port numbers from
            session_dal - DAL object to get the port numbers from

        Returns
            db - configuration database object, potentially updated if there were port conflicts
            session_dal - DAL object, potentially updated if there were port conflicts

        Raises:
            None
        """

        # Firstly, check if the file is read only. If so, we will only update the DAL
        configuration_file = db.active_database
        config_is_read_only: bool = file_is_read_only(
            get_full_db_path(configuration_file)
        )

        # Get the configuration ID to use in logging and potential DAL re-instantiation
        configuration_id = session_dal.id

        # Keep track of whether we made any changes, to avoid unnecessary DAL re-instantiation
        config_updated = False

        # Check that the address of the root controller is available, otherwise change
        # it to one that is available
        root_controller_host: str = session_dal.segment.controller.runs_on.runs_on.id
        root_controller_service: int = [
            service
            for service in session_dal.segment.controller.exposes_service
            if "_control" in service.id
        ][0]
        root_controller_port = root_controller_service.port
        if not is_port_available(root_controller_host, root_controller_port):
            config_updated = True
            if config_is_read_only:
                new_port = find_free_port(30000, 32767)
                root_controller_service.port = new_port
                self.log.debug(
                    f"Configuration file is read-only, updated root controller port in DAL to {new_port} to resolve conflict with occupied port {root_controller_port}"
                )
            else:
                new_port = set_rc_controller_port(configuration_file, configuration_id)
                strip_non_drunc_loggers()
                self.log.debug(
                    f"The root controller port at {root_controller_port} is occupied, updating it to {new_port}"
                )

        # If a local connectivity service is being used, perform the same checks
        # Temporarily removed to allow integration tests to pass without restructuring
        # Note - if infrastructure applications outside of the connectivity service are spawned, this will need to be adjusted.
        if session_dal.infrastructure_applications:  # Check if the own application needs to be spawned, or if an externally managed one is in use (e.g. if using ehn1 connectivity service or integration tests.)
            connectivity_service_host: str = session_dal.connectivity_service.host
            connectivity_service_port = session_dal.connectivity_service.service.port
            if not is_port_available(
                connectivity_service_host, connectivity_service_port
            ):
                config_updated = True
                if config_is_read_only:
                    new_port = find_free_port(30000, 32767)
                    session_dal.connectivity_service.service.port = new_port
                    self.log.debug(
                        f"Configuration file is read-only, updated connectivity service port in DAL to {new_port} to resolve conflict with occupied port {connectivity_service_port}"
                    )
                else:
                    new_port = set_connectivity_service_port(
                        configuration_file, configuration_id
                    )
                    strip_non_drunc_loggers()
                    self.log.debug(
                        f"The local connectivity service port at {connectivity_service_port} is occupied, updating it to {new_port}"
                    )

        if not config_updated:
            self.log.debug("Configuration did not require modifications.")
            return db, session_dal

        if config_is_read_only:
            # If the configuration file is read-only, we updated the DAL directly, so we can just
            # return it without re-instantiating
            self.log.debug(
                "Configuration required updates but file is read-only, returning updated DAL without changing the original file."
            )
            return db, session_dal
        else:
            # If the configuration file has been modified, instantiate a new DAL
            updated_db = conffwk.Configuration("oksconflibs:" + configuration_file)
            updated_session_dal = updated_db.get_dal(
                class_name="Session", uid=configuration_id
            )
            self.log.debug(
                "Configuration required updates and file is writable, re-instantiating DAL to reflect changes in the file."
            )
            return updated_db, updated_session_dal

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

            if connection_server == "localhost":
                resolved_server = resolve_localhost_to_hostname(connection_server)
                self.log.debug(
                    f"Resolved connection server 'localhost' to '{resolved_server}' to avoid K8s hairpinning."
                )
                connection_server = resolved_server

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
        Tries dynamic lookup via connectivity service first, then falls back
        to static OKS configuration.
        """
        try:
            top_controller_name = session_dal.segment.controller.id
        except AttributeError as e:
            self.log.error(f"Could not determine controller name from OKS: {e}")
            top_controller_name = "Unknown-Controller"  # Set a default

        def get_controller_address(session_dal, session_name):
            from drunc.process_manager.oks_parser import collect_variables

            env = {}
            collect_variables(session_dal.environment, env)

            # 1: Try dynamic lookup via Connectivity Service
            if csc:
                self.log.debug(
                    f"Attempting to discover controller '{top_controller_name}' via connectivity service at {connection_server}:{connection_port}"
                )
                try:
                    timeout = (
                        get_segment_lookup_timeout(session_dal.segment, 60) + 60
                    )  # root-controller timeout to find all its children + 60s for the root controller to start itself
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

                    address = uri.replace("grpc://", "")
                    self.log.debug(
                        f"Successfully discovered controller '{top_controller_name}' via connectivity service: {address}"
                    )
                    return address

                except ApplicationLookupUnsuccessful:
                    self.log.warning(
                        f"Connectivity service lookup failed: Application '{top_controller_name}' not found."
                    )
                    # Log the original failure details
                    self._log_controller_lookup_failure(
                        session_name,
                        top_controller_name,
                        connection_server,
                        connection_port,
                    )
                    self.log.warning(
                        "Falling back to static OKS configuration for address resolution."
                    )

                except Exception as e:
                    self.log.error(
                        f"An unexpected error occurred during connectivity service lookup: {e}. "
                        "Falling back to static OKS configuration."
                    )

            else:
                self.log.warning(
                    "Connectivity service client (csc) is not available. Using static OKS configuration only."
                )

            # 2: Fallback to static OKS configuration
            self.log.debug(
                "Attempting to resolve controller address from static OKS configuration."
            )

            port_number = None
            protocol = None
            service_found = None

            try:
                self.log.debug(
                    f"Top controller name from OKS config: '{top_controller_name}'"
                )

                if (
                    not hasattr(session_dal.segment.controller, "exposes_service")
                    or not session_dal.segment.controller.exposes_service
                ):
                    self.log.error(
                        f"Controller '{top_controller_name}' in OKS config has no 'exposes_service' relationship defined or it's empty."
                    )
                    return None

                self.log.debug(
                    f"Controller '{top_controller_name}' exposes services: {[s.id for s in session_dal.segment.controller.exposes_service]}"
                )

                # Get the first (and presumably only) control service linked
                service_found = next(
                    iter(session_dal.segment.controller.exposes_service), None
                )

                if service_found:
                    self.log.debug(
                        f"Found linked control service object with ID: '{service_found.id}'"
                    )
                    if (
                        hasattr(service_found, "port")
                        and service_found.port is not None
                    ):
                        port_number = service_found.port
                        self.log.debug(
                            f"Extracted port from service '{service_found.id}': {port_number}"
                        )
                    else:
                        self.log.error(
                            f"Service object '{service_found.id}' is missing the 'port' attribute or it's null."
                        )

                    if hasattr(service_found, "protocol") and service_found.protocol:
                        protocol = service_found.protocol
                        self.log.debug(
                            f"Extracted protocol from service '{service_found.id}': {protocol}"
                        )
                    else:
                        self.log.error(
                            f"Service object '{service_found.id}' is missing the 'protocol' attribute or it's empty."
                        )

                else:
                    self.log.error(
                        f"Could not retrieve the first service object from 'exposes_service' for controller '{top_controller_name}'."
                    )
                    return None

            except AttributeError as e:
                self.log.error(
                    f"Error accessing OKS configuration attributes: {e}. Check structure around session_dal.segment.controller."
                )
                return None
            except Exception as e:
                self.log.error(
                    f"Unexpected error during service discovery from OKS: {e}"
                )
                return None

            # Check if we successfully got a port and protocol
            if port_number is None or protocol is None:
                self.log.error(
                    f"Failed to extract valid port ({port_number}) or protocol ({protocol}) for service '{service_found.id if service_found else 'N/A'}'. Cannot determine controller address."
                )
                return None

            # Resolve the IP address of the host where the controller runs
            try:
                host_id = session_dal.segment.controller.runs_on.runs_on.id
                self.log.debug(f"Controller runs on host ID: '{host_id}'")
                ip = resolve_localhost_and_127_ip_to_network_ip(host_id)
                self.log.debug(f"Resolved host ID '{host_id}' to IP: {ip}")
            except AttributeError as e:
                self.log.error(
                    f"Error accessing OKS configuration attributes for host resolution: {e}. Check structure around session_dal.segment.controller.runs_on."
                )
                return None
            except Exception as e:
                self.log.error(f"Unexpected error during host IP resolution: {e}")
                return None

            if not ip:
                self.log.error(
                    f"Host ID '{host_id}' resolved to an empty or invalid IP address."
                )
                return None

            # If all checks passed, return the address
            final_address = f"{ip}:{port_number}"
            self.log.debug(
                f"Successfully resolved controller address from OKS config: {final_address}"
            )
            return final_address

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
                    top_controller_name, connection_server, connection_port
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
                yield response

            except grpc.RpcError as e:
                try:
                    error_details = extract_grpc_rich_error(e)
                    self.log.error(error_details)
                except Exception as extraction_error:
                    self.log.debug(
                        f"Could not extract rich error details from gRPC error: {extraction_error}",
                        exc_info=True,
                    )
                handle_grpc_error(e)

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
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )
            handle_grpc_error(e)

        return response

    def kill(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.kill(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )
            handle_grpc_error(e)

        return response

    def logs(self, request: LogRequest, timeout: int | float = 60) -> LogLines | None:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.logs(request, timeout=timeout)

            # Check if the response indicates a BadQuery error
            if response.flag == ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT:
                lines = response.lines
                if len(lines) == 1:
                    lines = lines[0]
                self.log.warning(f"Bad query for logs: {lines}")
                return None

            # Check for other error flags
            if response.flag == ResponseFlag.DRUNC_EXCEPTION_THROWN:
                self.log.error(f"Exception occurred on server: {response.lines}")
                return None

            return response

        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )
            handle_grpc_error(e)
            return None

    def ps(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.ps(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

            handle_grpc_error(e)

        return response

    def flush(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.flush(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

            handle_grpc_error(e)

        return response

    def restart(
        self, request: ProcessQuery, timeout: int | float = 60
    ) -> ProcessInstanceList:
        request.token.CopyFrom(self.token)

        try:
            response = self.stub.restart(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

            handle_grpc_error(e)

        return response

    def describe(self, timeout: int | float = 60) -> Description:
        request = Request(token=copy_token(self.token))

        try:
            response = self.stub.describe(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

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
