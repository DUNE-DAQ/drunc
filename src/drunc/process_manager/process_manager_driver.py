import getpass
import json
import os
import signal
import tempfile

import conffwk
from daqconf.consolidate import consolidate_db
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLine,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
)
from druncschema.process_manager_pb2_grpc import ProcessManagerStub
from druncschema.request_response_pb2 import Description

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.controller.utils import get_segment_lookup_timeout
from drunc.exceptions import DruncSetupException, DruncShellException
from drunc.process_manager.oks_parser import (
    collect_apps,
    collect_infra_apps,
    collect_variables,
)
from drunc.process_manager.utils import get_log_path, get_rte_script
from drunc.utils.configuration import find_configuration
from drunc.utils.shell_utils import GRPCDriver
from drunc.utils.utils import (
    get_control_type_and_uri_from_connectivity_service,
    host_is_local,
    resolve_localhost_and_127_ip_to_network_ip,
    resolve_localhost_to_hostname,
)


class ProcessManagerDriver(GRPCDriver):
    controller_address = ""

    def __init__(self, address: str, token, **kwargs):
        super(ProcessManagerDriver, self).__init__(
            name="process_manager.driver", address=address, token=token, **kwargs
        )
        self.log.debug("set up process_manager.driver")

    def create_stub(self, channel):
        return ProcessManagerStub(channel)

    async def _convert_oks_to_boot_request(
        self,
        oks_conf: str,
        user: str,
        session_dal,
        db,
        session_name: str,
        override_logs: bool,
    ) -> BootRequest:
        env = {
            "DUNEDAQ_SESSION": session_name,
        }

        apps = collect_apps(
            db,
            session_dal,
            session_dal.segment,
            env,
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

    async def boot(
        self,
        conf: str,
        user: str,
        session_name: str,
        log_level: str,
        override_logs: bool = True,
        **kwargs,
    ) -> ProcessInstance:
        self.log.info(f"Booting session [green]{session_name}[/green]")
        oks_conf = find_configuration(conf)

        with tempfile.NamedTemporaryFile(suffix=".data.xml", delete=True) as f:
            f.flush()
            f.seek(0)
            fname = f.name
            try:
                consolidate_db(oks_conf, f"{fname}")
            except Exception as e:
                self.log.critical(f"""\nInvalid configuration passed (cannot consolidate your configuration)
{e}
To debug it, close drunc and run the following command:

[yellow]oks_dump --files-only {oks_conf}[/]

""")
                return

        db = conffwk.Configuration(f"oksconflibs:{oks_conf}")
        session_dal = db.get_dal(class_name="Session", uid=session_name)

        async for br in self._convert_oks_to_boot_request(
            oks_conf=conf,
            user=user,
            session_dal=session_dal,
            session_name=session_name,
            db=db,
            override_logs=override_logs,
            **kwargs,
        ):
            yield await self.send_command_aio(
                "boot",
                data=br,
                outformat=ProcessInstance,
            )

        top_controller_name = session_dal.segment.controller.id

        def get_controller_address(session_dal, session_name):
            env = {}
            collect_variables(session_dal.environment, env)
            if session_dal.connectivity_service:
                connection_server = session_dal.connectivity_service.host
                connection_port = session_dal.connectivity_service.service.port
                csc = ConnectivityServiceClient(
                    session_name, f"{connection_server}:{connection_port}"
                )

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
                    self.log.error(f"""
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
""")
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
                self.log.warning(f"""This shell didn't connect to the {top_controller_name}.
To find the controller address, you can look up \'{top_controller_name}_control\' on http://{resolve_localhost_to_hostname(connection_server)}:{connection_port} (you may need a SOCKS proxy from outside CERN), or use the address from the logs as above. Then just connect this shell to the controller with:
[yellow]connect {{controller_address}}:{{controller_port}}>[/]
""")
            else:
                self.log.warning(
                    f"This shell didn't connect to the {top_controller_name}. You can use the connect command to connect to the controller."
                )
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)

    async def dummy_boot(
        self, user: str, session_name: str, n_processes: int, sleep: int, n_sleeps: int
    ):  # -> ProcessInstance:
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
            breq = BootRequest(
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
            self.log.debug(f"{breq=}\n\n")

            yield await self.send_command_aio(
                "boot",
                data=breq,
                outformat=ProcessInstance,
            )

    async def terminate(
        self,
    ) -> ProcessInstanceList:
        return await self.send_command_aio("terminate", outformat=ProcessInstanceList)

    async def kill(self, query: ProcessQuery) -> ProcessInstance:
        return await self.send_command_aio(
            "kill",
            data=query,
            outformat=ProcessInstanceList,
        )

    async def logs(self, req: LogRequest) -> LogLine:
        async for stream in self.send_command_for_aio(
            "logs",
            data=req,
            outformat=LogLine,
        ):
            yield stream

    async def ps(self, query: ProcessQuery) -> ProcessInstanceList:
        return await self.send_command_aio(
            "ps",
            data=query,
            outformat=ProcessInstanceList,
        )

    async def flush(self, query: ProcessQuery) -> ProcessInstanceList:
        return await self.send_command_aio(
            "flush",
            data=query,
            outformat=ProcessInstanceList,
        )

    async def restart(self, query: ProcessQuery) -> ProcessInstance:
        return await self.send_command_aio(
            "restart",
            data=query,
            outformat=ProcessInstance,
        )

    async def describe(self) -> Description:
        return await self.send_command_aio(
            "describe",
            outformat=Description,
        )
