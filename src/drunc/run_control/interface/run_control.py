from __future__ import annotations

import getpass
import conffwk

import concurrent
import signal
import types
from collections.abc import Callable
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event

from drunc.utils.grpc_utils import ServerUnreachable

import sys

from urllib.parse import ParseResult, urlparse

import click
import grpc
from druncschema.run_control_pb2_grpc import add_RunControlServicer_to_server

from drunc.exceptions import DruncSetupException
from drunc.grpc_settings import (
    MANAGER_SERVER_GRPC_CONFIG,
    MANAGER_SERVER_GRPC_MAX_WORKERS,
)
from drunc.run_control.run_control import RunControl
from drunc.utils.grpc_utils import RichErrorServerInterceptor
from drunc.utils.utils import (
    get_logger,
    get_root_logger,
    resolve_localhost_and_127_ip_to_network_ip,
)

from drunc.process_manager.utils import get_pm_type_from_name, validate_k8s_session_name

from drunc.process_manager.configuration import (
    ProcessManagerRunningMode,
    ProcessManagerTypes,
    get_process_manager_configuration,
    validate_pm_config,
)

_cleanup_coroutines: list[Callable[[], None]] = []


def run_rc(
    rc_address: str,
    ready_event: Event | None = None,
    generated_port: Synchronized[int] | None = None,
):
    appName = "run_control"
    log = get_logger(logger_name=appName, rich_handler=True)

    log.info("Running [green]run_rc[/green]")

    rc = RunControl()  # rcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrc
    log.info("Set up Run Controller")

    server: grpc.Server | None = None

    def serve(address: str) -> None:
        address = resolve_localhost_and_127_ip_to_network_ip(address)
        log.info("serving")

        if not address:
            raise DruncSetupException(
                "The address on which to expect commands/send status wasn't specified"
            )
        nonlocal server
        server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=MANAGER_SERVER_GRPC_MAX_WORKERS
            ),
            options=MANAGER_SERVER_GRPC_CONFIG,
            interceptors=[RichErrorServerInterceptor()],
        )

        add_RunControlServicer_to_server(rc, server)
        port = server.add_insecure_port(address)
        if generated_port is not None:
            generated_port.value = port

        server.start()
        host = address.split(":")[0]
        log.info(
            f"run_control communicating through address [bold green]{host}:{port}[/bold green]"
        )

        _cleanup_coroutines.append(server_shutdown)
        if ready_event is not None:
            ready_event.set()

        server.wait_for_termination()

    def server_shutdown() -> None:
        nonlocal server
        if server:
            log.info("Shutting down the run control server")
            server.stop(1)
            server = None
        return

    def handle_sigterm(signum: int, frame: types.FrameType | None) -> None:
        log.info("SIGTERM received, shutting down server...")
        server_shutdown()
        return

    # Register sigterm handler
    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        log.info("Serving run_control")
        serve(rc_address)
    except Exception as e:
        log.error("There was an exception")
        log.exception(e)
    finally:
        if _cleanup_coroutines:
            for coroutine in _cleanup_coroutines:
                coroutine()


@click.command()
@click.argument("rc-port", type=int)
# @click.argument("process-manager", type=str, nargs=1)
# @click.argument("configuration-file", type=str, nargs=1)
# @click.argument("configuration-id", type=str, nargs=1)
# @click.argument("session-name", type=str, nargs=1)
# @click.option(
#     "-o/-no",
#     "--override-logs/--no-override-logs",
#     type=bool,
#     default=True,
#     help="Override logs, if --no-override-logs filenames have the timestamp of the run.",
# )  # For production, change default to false/remove it
# @click.option(
#     "-nsb",
#     "--no-stop-error-batch-mode",
#     is_flag=True,
#     default=False,
#     help=(
#         "The default behaviour of the unified shell is to exit if the root-controller "
#         "of the sessions is in error. This option will allow the unified shell to "
#         "continue executing commands, mainly for use in testing scenarios."
#     ),
# )
@click.pass_context
def rc_cli(
    ctx: click.core.Context,
    rc_port: int,
    # process_manager: str,
    # configuration_file: str,
    # configuration_id: str,
    # session_name: str,
    # no_stop_error_batch_mode: bool,
    # override_logs: bool,
):
    get_root_logger("info")

    ctx.obj.log = get_logger("rc_cli_thing", rich_handler=True)

    ctx.obj.log.info("HELLO WORLD")




    # rc_cli_logger = get_logger(logger_name="RC_CLI_LOGGER", rich_handler=True)

    ### You will need to get the pprocess manager configuration first. Also note! We must be able to support the run control service to connect tto a process manager service!

    ## lets start with the basics, lets get the pm configuration connected

    ## oohh I see it does the connectiy connectty first

    ## lets start with it being an address first..

    # process_manager_url: ParseResult = urlparse(process_manager)

    # This is exactly copy pasted from the unified shell. Note we should probably synchronise this later on..

    # internal_pm = False

    # if process_manager_url.scheme == "grpc":  # i.e. if it's an address
    #     internal_pm = False
    #     ctx.obj.log.info(
    #         f"[green]Connecting to an existing process manager[/] at the address {process_manager_url.netloc}"
    #     )

    #     ctx.obj.reset(address_pm=process_manager_url.netloc)

    #     pm_type = ProcessManagerTypes[
    #         ctx.obj.get_driver("process_manager").describe().type
    #     ]

    #     ctx.obj.log.info(
    #         f"[green]Connected to the {pm_type.name} process manager[/] running at address {process_manager_url.netloc}"
    #     )
    # else:
    #     raise ValueError("Unsupported for now")

    # if not validate_k8s_session_name(session_name) and (
    #     pm_type == ProcessManagerTypes.K8s
    # ):
    #     ctx.obj.log.error(
    #         f"[red]Invalid session/namespace name [bold]({session_name})[/bold][/red]. "
    #         "Must match RFC1123 label: lowercase alphanumeric or '-', start/end with "
    #         "alphanumeric, max 63 chars."
    #     )
    #     sys.exit(1)

    # # add configuration items
    # ctx.obj.configuration_file = (
    #     lambda path: (
    #         path if path.startswith("oksconflibs:") else f"oksconflibs:{path}"
    #     )
    # )(configuration_file)
    # ctx.obj.configuration_id = configuration_id
    # ctx.obj.session_name = session_name
    # ctx.obj.no_stop_error_batch_mode = no_stop_error_batch_mode
    # ctx.obj.override_logs = override_logs

    # # Get the session DAL
    # db = conffwk.Configuration(ctx.obj.configuration_file)
    # session_dal = db.get_dal(class_name="Session", uid=ctx.obj.configuration_id)
    # app_log_path = session_dal.log_path

    # ctx.obj.log.info("Setting up the controller interface")

    # # Keep track of whether the session uses a local connectivity service
    # ctx.obj.session_uses_local_connectivity_service = (
    #     session_dal.connectivity_service.host == "localhost"
    # )

    # # Run a simple command (describe) to check the connection with the process manager

    # ctx.obj.get_driver("process_manager").send_log(
    #     f"{getpass.getuser()} connected from run control (!!)"
    # )

    # running_mode = ProcessManagerRunningMode.Subprocess #not sure why this existrs

    # internal_pm = False
    # running_mode = ProcessManagerRunningMode.Subprocess
    # ctx.obj.reset(address_pm=process_manager_url.netloc)
    # pm_type = ProcessManagerTypes[
    #     ctx.obj.get_driver("process_manager").describe().type
    # ]
    # ctx.obj.log.info(
    #     f"[green]Connected to the {pm_type.name} process manager[/] running at address {process_manager_url.netloc}"
    # )

    # There is a K8s validation step here first

    # pm_conf = get_process_manager_configuration(pm_conf) #import the definition and see where pm_conf can even come from






    run_rc(rc_address=f"0.0.0.0:{rc_port}")
