from __future__ import annotations

import concurrent
import signal
import types
from collections.abc import Callable
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event

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
@click.argument("process-manager", type=str, nargs=1)
def rc_cli(
        ctx: click.core.Context,
        rc_port: int, 
        process_manager:str
    ):
    get_root_logger("info")

    ctx.obj.log.info("HELLO WORLD")
    # rc_cli_logger = get_logger(logger_name="RC_CLI_LOGGER", rich_handler=True)
    

    ### You will need to get the pprocess manager configuration first. Also note! We must be able to support the run control service to connect tto a process manager service!

    ## lets start with the basics, lets get the pm configuration connected 


    ## oohh I see it does the connectiy connectty first

    ## lets start with it being an address first.. 

    process_manager_url: ParseResult = urlparse(process_manager)

    # This is exactly copy pasted from the unified shell. Note we should probably synchronise this later on..

    if process_manager_url.scheme == "grpc":  # i.e. if it's an address
        
        ctx.obj.log.info(
            f"[green]Connecting to an existing process manager[/] at the address {process_manager_url.netloc}"
        )

        ctx.obj.reset(address_pm=process_manager_url.netloc)


        ctx.obj.log.warning(ctx.obj.get_driver("process_manager").describe().type)


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
    else: 
        pass

    # There is a K8s validation step here first


    
    # pm_conf = get_process_manager_configuration(pm_conf) #import the definition and see where pm_conf can even come from


    
    run_rc(rc_address=f"0.0.0.0:{rc_port}")
