import asyncio
import getpass
import logging
import multiprocessing as mp
import os
import sys
from time import sleep
from urllib.parse import urlparse

import click
import click_shell
import conffwk

from drunc.controller.configuration import ControllerConfHandler
from drunc.controller.interface.commands import (
    connect,
    exclude,
    include,
    status,
    surrender_control,
    take_control,
    wait,
    who_am_i,
    who_is_in_charge,
)
from drunc.controller.interface.shell_utils import generate_fsm_command
from drunc.controller.stateful_node import StatefulNode
from drunc.exceptions import DruncSetupException
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.utils import convert_fsm_transition
from drunc.process_manager.configuration import get_process_manager_configuration
from drunc.process_manager.interface.commands import (
    dummy_boot,
    flush,
    kill,
    logs,
    ps,
    restart,
    terminate,
)
from drunc.process_manager.interface.process_manager import run_pm
from drunc.process_manager.utils import get_log_path, get_pm_conf_name_from_dir
from drunc.unified_shell.commands import boot
from drunc.utils.configuration import OKSKey, find_configuration, parse_conf_url
from drunc.utils.utils import (
    get_logger,
    ignore_sigint_sighandler,
    log_levels,
    pid_info_str,
    setup_root_logger,
    setup_standard_loggers,
)


@click_shell.shell(
    prompt="drunc-unified-shell > ",
    chain=True,
    hist_file=os.path.expanduser("~") + "/.drunc-unified-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.argument("process-manager", type=str, nargs=1)
@click.argument("boot-configuration", type=str, nargs=1)
@click.argument("session-name", type=str, nargs=1)
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    type=bool,
    default=True,
    help="Override logs, if --no-override-logs filenames have the timestamp of the run.",
)  # For production, change default to false/remove it
@click.option(
    "-lp",
    "--log-path",
    type=str,
    default=None,
    help="Log path of process_manager logs.",
)
@click.pass_context
def unified_shell(
    ctx,
    process_manager: str,
    boot_configuration: str,
    session_name: str,
    log_level: str,
    override_logs: bool,
    log_path: str,
) -> None:
    # Set up the drunc and unified_shell loggers
    setup_root_logger(log_level)
    setup_standard_loggers()
    unified_shell_log = get_logger(logger_name="unified_shell", rich_handler=True)
    unified_shell_log.debug("Set up [green]unified_shell[/green] logger")
    unified_shell_log.debug(pid_info_str())

    process_manager_url = urlparse(process_manager)
    if (
        process_manager_url.scheme != "grpc"
    ):  # slightly hacky to see if the process manager is an address
        internal_pm = True
    else:
        internal_pm = False

    # Set up process_manager logger
    conf = find_configuration(boot_configuration)
    ctx.obj.boot_configuration = conf
    ctx.obj.session_name = session_name
    db = conffwk.Configuration(f"oksconflibs:{conf}")
    app_log_path = db.get_dal(class_name="Session", uid=session_name).log_path

    pm_log_path = get_log_path(
        user=getpass.getuser(),
        session_name=get_pm_conf_name_from_dir(process_manager),
        application_name="process_manager",
        override_logs=override_logs,
        app_log_path=app_log_path,
    )
    process_manager_log = get_logger(
        logger_name="process_manager",
        log_file_path=pm_log_path,
        override_log_file=internal_pm,
        rich_handler=True,
    )
    process_manager_log.debug("Set up [green]process_manager[/green] logger")
    unified_shell_log.info(
        f'Setting up to use [green]process_manager[/green] with configuration [green]{process_manager}[/green] and [green]session "{session_name}"[/green] from [green]{boot_configuration}[/green]'
    )

    if internal_pm:
        unified_shell_log.debug(
            f"Spawning [green]process_manager[/green] with configuration {process_manager}"
        )
        # Check if process_manager is a packaged config
        process_manager_conf_file = get_process_manager_configuration(process_manager)

        ready_event = mp.Event()
        port = mp.Value("i", 0)

        unified_shell_log.debug(
            "Starting [green]process_manager[/green] as separate process"
        )
        ctx.obj.pm_process = mp.Process(
            target=run_pm,
            kwargs={
                "pm_conf": process_manager_conf_file,
                "pm_address": "localhost:0",
                "override_logs": override_logs,
                "log_level": log_level,
                "ready_event": ready_event,
                "signal_handler": ignore_sigint_sighandler,
                # sigint gets sent to the PM, so we need to ignore it, otherwise everytime the user ctrl-c on the shell, the PM goes down
                "generated_port": port,
            },
        )
        ctx.obj.pm_process.start()
        unified_shell_log.debug("[green]process_manager[/green] started")

        for _ in range(100):
            if ready_event.is_set():
                break
            sleep(0.1)
        if not ready_event.is_set():
            raise DruncSetupException(
                "[green]process_manager[/green] [red]did not start in time[/red]"
            )
        process_manager_address = f"localhost:{port.value}"

    else:  # user provided an address
        process_manager_address = process_manager.replace(
            "grpc://", ""
        )  # remove the grpc scheme
        unified_shell_log.info(
            f"[green]unified_shell[/green] connected to the [green]process_manager[/green] ([green]{process_manager}[/green]) at address [green]{process_manager_address}[/green]"
        )

    unified_shell_log.debug(
        f"[green]process_manager[/green] started, communicating through address [green]{process_manager_address}[/green]"
    )
    ctx.obj.reset(address_pm=process_manager_address)

    desc = None
    try:
        unified_shell_log.debug("Runnning [green]describe[/green]")
        desc = asyncio.get_event_loop().run_until_complete(
            ctx.obj.get_driver().describe()
        )
        desc = desc.data
    except Exception as e:
        unified_shell_log.error(
            f"[red]Could not connect to the process manager at the address[/red] [green]{process_manager_address}[/]"
        )
        if internal_pm and not ctx.obj.pm_process.is_alive():
            unified_shell_log.error(
                f"[red]The process_manager is dead[/red], exit code {ctx.obj.pm_process.exitcode}"
            )
        unified_shell_log.exception(e)
        sys.exit()

    if desc.HasField("broadcast"):
        unified_shell_log.debug("Broadcasting")
        ctx.obj.start_listening_pm(
            broadcaster_conf=desc.broadcast,
        )

    def cleanup():
        unified_shell_log.debug("Cleanup")
        ctx.obj.terminate()
        if internal_pm:
            ctx.obj.pm_process.terminate()
            ctx.obj.pm_process.join()
        logging.shutdown()

    ctx.call_on_close(cleanup)

    unified_shell_log.debug(
        "Adding [green]unified_shell[/green] commands to the context"
    )
    ctx.command.add_command(boot, "boot")

    unified_shell_log.debug(
        "Adding [green]process_manager[/green] commands to the context"
    )
    ctx.command.add_command(kill, "kill")
    ctx.command.add_command(terminate, "terminate")
    ctx.command.add_command(flush, "flush")
    ctx.command.add_command(logs, "logs")
    ctx.command.add_command(restart, "restart")
    ctx.command.add_command(ps, "ps")
    ctx.command.add_command(dummy_boot, "dummy_boot")

    # Not particularly proud of this...
    # We instantiate a stateful node which has the same configuration as the one from this session
    # Let's do this
    unified_shell_log.debug("Retrieving the session database")
    db = conffwk.Configuration(f"oksconflibs:{ctx.obj.boot_configuration}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)

    conf_path, conf_type = parse_conf_url(f"oksconflibs:{ctx.obj.boot_configuration}")
    controller_name = session_dal.segment.controller.id
    unified_shell_log.debug("Initializing the [green]ControllerConfHandler[/green]")
    controller_configuration = ControllerConfHandler(
        type=conf_type,
        data=conf_path,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=controller_name,
            session=session_name,  # some of the function for enable/disable require the full dal of the session
        ),
    )

    fsm_logger = get_logger("controller.FSM")
    fsm_logger.setLevel("ERROR")
    fsm_conf_logger = get_logger("controller.FSMConfHandler")
    fsm_conf_logger.setLevel("ERROR")

    unified_shell_log.debug("Initializing the [green]FSM[/green]")
    fsmch = FSMConfHandler(
        data=controller_configuration.data.controller.fsm,
    )

    unified_shell_log.debug("Initializing the [green]StatefulNode[/green]")
    stateful_node = StatefulNode(
        fsm_configuration=fsmch,
        broadcaster=None,
    )

    unified_shell_log.debug(
        "Retrieving the transitions from the [green]StatefulNode[/green]"
    )
    transitions = convert_fsm_transition(stateful_node.get_all_fsm_transitions())
    fsm_logger.setLevel(log_level)
    fsm_conf_logger.setLevel(log_level)
    # End of shameful code

    unified_shell_log.debug(
        "Adding [green]controller[/green] commands to the click context"
    )
    for transition in transitions.commands:
        ctx.command.add_command(
            *generate_fsm_command(ctx.obj, transition, controller_name)
        )

    ctx.command.add_command(status, "status")
    ctx.command.add_command(connect, "connect")
    ctx.command.add_command(take_control, "take-control")
    ctx.command.add_command(surrender_control, "surrender-control")
    ctx.command.add_command(who_am_i, "whoami")
    ctx.command.add_command(who_is_in_charge, "who-is-in-charge")
    ctx.command.add_command(include, "include")
    ctx.command.add_command(exclude, "exclude")
    ctx.command.add_command(wait, "wait")

    unified_shell_log.info(
        "[green]unified_shell[/green] ready with [green]process_manager[/green] and [green]controller[/green] commands"
    )
