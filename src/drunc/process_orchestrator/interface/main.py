import asyncio
import getpass
import os

import click
import grpc
from drunc_core.exceptions import DruncSetupException
from drunc_core.utils.configuration import parse_conf_url
from drunc_core.utils.utils import (
    create_logger_handler,
    get_logger,
    log_levels,
    parent_death_pact,
    resolve_localhost_and_127_ip_to_network_ip,
    setup_root_logger,
)
from drunc_messages.process_orchestrator_pb2_grpc import (
    add_ProcessOrchestratorServicer_to_server,
)

from drunc.process_orchestrator.configuration import (
    ProcessOrchestratorConfHandler,
    get_process_orchestrator_configuration,
)
from drunc.process_orchestrator.factory import get_process_orchestrator
from drunc.process_orchestrator.utils import (
    get_log_path,
    get_process_orchestrator_conf_name_from_dir,
)

_cleanup_coroutines = []


def run_process_orchestrator(
    process_orchestrator_conf: str,
    process_orchestrator_address: str,
    log_level: str,
    override_logs: bool,
    log_path: str = None,
    ready_event: bool = None,
    signal_handler: bool = None,
    generated_port: bool = None,
) -> None:
    appName = "process_orchestrator"
    process_orchestrator_conf_name = get_process_orchestrator_conf_name_from_dir(
        process_orchestrator_conf
    )  # Treating the process_orchestrator conf data filename as the session

    log_path = get_log_path(
        user=getpass.getuser(),
        session_name=process_orchestrator_conf_name,
        application_name=appName,
        override_logs=override_logs,
        app_log_path=log_path,
    )
    log = get_logger(logger_name=appName)
    create_logger_handler(
        log_file_path=log_path,
        rich_handler=True,
    )

    log.debug("Running [green]run_process_orchestrator[/green]")
    if signal_handler is not None:
        signal_handler()

    parent_death_pact()  # If the parent dies (for example unified shell), we die too

    log.debug(
        f"Using '{process_orchestrator_conf}' as the ProcessOrchestrator configuration"
    )

    conf_path, conf_type = parse_conf_url(process_orchestrator_conf)
    process_orchestrator_conf_handler = ProcessOrchestratorConfHandler(
        log_path=log_path, type=conf_type, data=conf_path.split(":")[1]
    )

    for key, value in process_orchestrator_conf_handler.data.environment.items():
        os.environ[key] = value

    process_orchestrator = get_process_orchestrator(
        process_orchestrator_conf_handler, name="process_orchestrator"
    )
    log.debug("Setup up ProcessOrchestrator")

    loop = asyncio.get_event_loop()

    async def serve(address: str) -> None:
        address = resolve_localhost_and_127_ip_to_network_ip(address)
        log.debug("serve called")
        if not address:
            raise DruncSetupException(
                "The address on which to expect commands/send status wasn't specified"
            )
        server = grpc.aio.server()
        add_ProcessOrchestratorServicer_to_server(process_orchestrator, server)
        port = server.add_insecure_port(address)
        if generated_port is not None:
            generated_port.value = port

        await server.start()
        # hostname = socket.gethostname()
        host = address.split(":")[0]
        log.info(
            f"process_orchestrator communicating through address [bold green]{host}:{port}[/bold green]"
        )  # bold as part of the address was already formatting, couldn't figure out why

        async def server_shutdown():
            log.warning("Starting shutdown...")
            # Shuts down the server with 5 seconds of grace period. During the
            # grace period, the server won't accept new connections and allow
            # existing RPCs to continue within the grace period.
            await server.stop(5)
            process_orchestrator._terminate()

        _cleanup_coroutines.append(server_shutdown())
        if ready_event is not None:
            ready_event.set()
        await server.wait_for_termination()

    try:
        log.debug("Serving process_orchestrator")
        loop.run_until_complete(serve(process_orchestrator_address))
    except Exception as e:
        log.error("Serving the ProcessOrchestrator received an Exception")
        log.exception(e)
    finally:
        if _cleanup_coroutines:
            log.info("Clearing coroutines")
            loop.run_until_complete(*_cleanup_coroutines)
        loop.close()


@click.command()
@click.argument("process-orchestrator-conf", type=str)
@click.argument("process-orchestrator-port", type=int)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default=os.getenv("DRUNC_LOG_LEVEL", "INFO"),
    help="Set the log level, if not set, it will be set to the environment variable DRUNC_LOG_LEVEL, if that variable is not set, it will be set to INFO",
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
    help="Log path of process_orchestrator logs.",
)
def process_orchestrator_cli(
    process_orchestrator_conf: str,
    process_orchestrator_port: int,
    log_level: str,
    override_logs: bool,
    log_path: str,
) -> None:
    setup_root_logger(log_level)
    process_orchestrator_conf = get_process_orchestrator_configuration(
        process_orchestrator_conf
    )
    run_process_orchestrator(
        process_orchestrator_conf=process_orchestrator_conf,
        process_orchestrator_address=f"0.0.0.0:{process_orchestrator_port}",
        log_level=log_level,
        override_logs=override_logs,
        log_path=log_path,
    )
