import click
import click_shell
from drunc.utils.utils import log_levels
import os
from drunc.utils.utils import validate_command_facility
import pathlib
from drunc.process_manager.interface.cli_argument import validate_conf_string
from urllib.parse import urlparse
from rich import print as rprint
import logging

@click_shell.shell(prompt='drunc-unified-shell > ', chain=True, hist_file=os.path.expanduser('~')+'/.drunc-unified-shell.history')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.argument('process-manager', type=str, nargs=1)
@click.argument('boot-configuration', type=str, nargs=1)
@click.argument('session-name', type=str, nargs=1)
@click.pass_context
def unified_shell(
    ctx,
    process_manager:str,
    boot_configuration:str,
    session_name:str,
    log_level:str,
) -> None:

    from drunc.utils.utils import update_log_level, pid_info_str, ignore_sigint_sighandler
    update_log_level(log_level)
    from logging import getLogger
    logger = getLogger('unified_shell')
    logger.debug(pid_info_str())



    url_process_manager = urlparse(process_manager)
    external_pm = True

    if url_process_manager.scheme != 'grpc': # slightly hacky to see if the process manager is an address
        rprint(f"Spawning a process manager with configuration [green]{process_manager}[/green]")
        external_pm = False
        # Check if process_manager is a packaged config
        from drunc.process_manager.configuration import get_process_manager_configuration
        process_manager = get_process_manager_configuration(process_manager)

        from drunc.process_manager.interface.process_manager import run_pm
        import multiprocessing as mp
        ready_event = mp.Event()
        port = mp.Value('i', 0)

        ctx.obj.pm_process = mp.Process(
            target = run_pm,
            kwargs = {
                "pm_conf": process_manager,
                "pm_address": "localhost:0",
                "log_level": log_level,
                "ready_event": ready_event,
                "signal_handler": ignore_sigint_sighandler,
                # sigint gets sent to the PM, so we need to ignore it, otherwise everytime the user ctrl-c on the shell, the PM goes down
                "generated_port": port,
            },
        )
        ctx.obj.print(f'Starting process manager with configuration {process_manager}')
        ctx.obj.pm_process.start()


        from time import sleep
        for _ in range(100):
            if ready_event.is_set():
                break
            sleep(0.1)

        if not ready_event.is_set():
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException('Process manager did not start in time')

        import socket
        process_manager_address = f'localhost:{port.value}'

    else: # user provided an address
        rprint(f"Connecting to process manager at [green]{process_manager}[/green]")
        process_manager_address = process_manager.replace('grpc://', '') # remove the grpc scheme

    ctx.obj.reset(
        address_pm = process_manager_address,
    )

    desc = None

    try:
        import asyncio
        desc = asyncio.get_event_loop().run_until_complete(
            ctx.obj.get_driver().describe()
        )
        desc = desc.data

    except Exception as e:
        ctx.obj.critical(f'Could not connect to the process manager at the address [green]{process_manager_address}[/]', extra={'markup': True})
        if not external_pm and not ctx.obj.pm_process.is_alive():
            ctx.obj.critical(f'The process manager is dead, exit code {ctx.obj.pm_process.exitcode}')
        if logging.DEBUG == logging.root.level:
            raise e
        else:
            exit()

    from drunc.utils.configuration import find_configuration
    ctx.obj.boot_configuration = find_configuration(boot_configuration)
    ctx.obj.session_name = session_name


    ctx.obj.info(f'{process_manager_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    if desc.HasField('broadcast'):
        ctx.obj.start_listening_pm(
            broadcaster_conf = desc.broadcast,
        )

    def cleanup():
        ctx.obj.terminate()
        if not external_pm:
            ctx.obj.pm_process.terminate()
            ctx.obj.pm_process.join()

    ctx.call_on_close(cleanup)

    from drunc.unified_shell.commands import boot
    ctx.command.add_command(boot, 'boot')

    from drunc.process_manager.interface.commands import kill, terminate, flush, logs, restart, ps, dummy_boot
    ctx.command.add_command(kill, 'kill')
    ctx.command.add_command(terminate, 'terminate')
    ctx.command.add_command(flush, 'flush')
    ctx.command.add_command(logs, 'logs')
    ctx.command.add_command(restart, 'restart')
    ctx.command.add_command(ps, 'ps')
    ctx.command.add_command(dummy_boot, 'dummy_boot')

    # Not particularly proud of this...
    # We instantiate a stateful node which has the same configuration as the one from this session
    # Let's do this
    import conffwk

    db = conffwk.Configuration(f"oksconflibs:{ctx.obj.boot_configuration}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)

    from drunc.utils.configuration import parse_conf_url, OKSKey
    conf_path, conf_type = parse_conf_url(f"oksconflibs:{ctx.obj.boot_configuration}")
    controller_name = session_dal.segment.controller.id
    from drunc.controller.configuration import ControllerConfHandler
    controller_configuration = ControllerConfHandler(
        type = conf_type,
        data = conf_path,
        oks_key = OKSKey(
            schema_file='schema/confmodel/dunedaq.schema.xml',
            class_name="RCApplication",
            obj_uid=controller_name,
            session=session_name, # some of the function for enable/disable require the full dal of the session
        ),
    )

    from drunc.fsm.configuration import FSMConfHandler
    fsm_logger = getLogger("FSM")
    fsm_log_level = fsm_logger.level
    fsm_logger.setLevel("ERROR")
    fsm_conf_logger = getLogger("FSMConfHandler")
    fsm_conf_log_level = fsm_conf_logger.level
    fsm_conf_logger.setLevel("ERROR")

    fsmch = FSMConfHandler(
        data = controller_configuration.data.controller.fsm,
    )

    from drunc.controller.stateful_node import StatefulNode
    stateful_node = StatefulNode(
        fsm_configuration = fsmch,
        broadcaster = None,
    )

    from drunc.fsm.utils import convert_fsm_transition

    transitions = convert_fsm_transition(stateful_node.get_all_fsm_transitions())
    fsm_logger.setLevel(fsm_log_level)
    fsm_conf_logger.setLevel(fsm_conf_log_level)
    # End of shameful code

    from drunc.controller.interface.shell_utils import generate_fsm_command
    for transition in transitions.commands:
        ctx.command.add_command(*generate_fsm_command(ctx.obj, transition, controller_name))


    from drunc.controller.interface.commands import (
        list_transitions, ls, status, connect, take_control, surrender_control, who_am_i, who_is_in_charge, fsm, include, exclude, wait
    )

    ctx.command.add_command(list_transitions, 'list-transitions')
    ctx.command.add_command(ls, 'ls')
    ctx.command.add_command(status, 'status')
    ctx.command.add_command(connect, 'connect')
    ctx.command.add_command(take_control, 'take-control')
    ctx.command.add_command(surrender_control, 'surrender-control')
    ctx.command.add_command(who_am_i, 'whoami')
    ctx.command.add_command(who_is_in_charge, 'who-is-in-charge')
    ctx.command.add_command(include, 'include')
    ctx.command.add_command(exclude, 'exclude')
    ctx.command.add_command(wait, 'wait')
