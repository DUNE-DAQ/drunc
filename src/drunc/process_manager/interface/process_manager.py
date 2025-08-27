import concurrent
import getpass
import os

import click
import grpc
from druncschema.process_manager_pb2_grpc import add_ProcessManagerServicer_to_server

from drunc.exceptions import DruncSetupException
from drunc.process_manager.configuration import (
    ProcessManagerConfHandler,
    get_process_manager_configuration,
)
from drunc.process_manager.process_manager import ProcessManager
from drunc.process_manager.utils import get_log_path
from drunc.utils.configuration import parse_conf_url
from drunc.utils.utils import (
    create_logger_handler,
    get_logger,
    log_levels,
    parent_death_pact,
    resolve_localhost_and_127_ip_to_network_ip,
    setup_root_logger,
)

_cleanup_coroutines = []


def run_pm(
    pm_conf: str,
    pm_address: str,
    log_level: str,
    override_logs: bool,
    log_path: str = None,
    ready_event: bool = None,
    signal_handler: bool = None,
    generated_port: bool = None,
) -> None:
    appName = "process_manager"
    log = get_logger(logger_name=appName)

    log.debug("Running [green]run_pm[/green]")
    if signal_handler is not None:
        signal_handler()

    parent_death_pact()  # If the parent dies (for example unified shell), we die too

    log.debug(f"Using '{pm_conf}' as the ProcessManager configuration")

    conf_path, conf_type = parse_conf_url(pm_conf)
    pmch = ProcessManagerConfHandler(
        log_path=log_path, type=conf_type, data=conf_path.split(":")[1]
    )

    log_path = get_log_path(
        user=getpass.getuser(),
        session_name=pmch.data.type.name,
        application_name=appName,
        override_logs=override_logs,
        app_log_path=log_path,
    )
    create_logger_handler(
        log_file_path=log_path,
        rich_handler=True,
    )

    for key, value in pmch.data.environment.items():
        os.environ[key] = value

    pm = ProcessManager.get(pmch, name="process_manager")
    log.debug("Setup up ProcessManager")

    def serve(address: str) -> None:
        address = resolve_localhost_and_127_ip_to_network_ip(address)
        log.debug("serve called")
        if not address:
            raise DruncSetupException(
                "The address on which to expect commands/send status wasn't specified"
            )
        server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
        add_ProcessManagerServicer_to_server(pm, server)
        port = server.add_insecure_port(address)
        if generated_port is not None:
            generated_port.value = port

        server.start()
        # hostname = socket.gethostname()
        host = address.split(":")[0]
        log.info(
            f"process_manager communicating through address [bold green]{host}:{port}[/bold green]"
        )  # bold as part of the address was already formatting, couldn't figure out why

        def server_shutdown():
            log.warning("Starting shutdown...")
            # Shuts down the server with 5 seconds of grace period. During the
            # grace period, the server won't accept new connections and allow
            # existing RPCs to continue within the grace period.
            server.stop(5)
            pm._terminate_impl()

        _cleanup_coroutines.append(server_shutdown)
        if ready_event is not None:
            ready_event.set()
        server.wait_for_termination()

    try:
        log.debug("Serving process_manager")
        serve(pm_address)
    except Exception as e:
        log.error("Serving the ProcessManager received an Exception")
        log.exception(e)
    finally:
        if _cleanup_coroutines:
            for coroutine in _cleanup_coroutines:
                coroutine()


@click.command()
@click.argument("pm-conf", type=str)
@click.argument("pm-port", type=int)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    type=bool,
    default=True,
    help="Override logs, if --no-override-logs filenames have the timestamp of the run.",
)
@click.option(
    "-lp",
    "--log-path",
    type=str,
    default=None,
    help="Log path of process_manager logs.",
)
def process_manager_cli(
    pm_conf: str, pm_port: int, log_level: str, override_logs: bool, log_path: str
) -> None:
    setup_root_logger(log_level)
    pm_conf = get_process_manager_configuration(pm_conf)
    run_pm(
        pm_conf=pm_conf,
        pm_address=f"0.0.0.0:{pm_port}",
        log_level=log_level,
        override_logs=override_logs,
        log_path=log_path,
    )
