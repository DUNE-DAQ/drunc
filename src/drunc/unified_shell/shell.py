import click
import click_shell
from drunc.utils.utils import log_levels
import os
from drunc.utils.utils import validate_command_facility
import pathlib
from drunc.process_manager.interface.cli_argument import validate_conf_string

@click_shell.shell(prompt='drunc-unified-shell > ', chain=True, hist_file=os.path.expanduser('~')+'/.drunc-unified-shell.history')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.argument('process-manager-configuration', type=str, nargs=1)
@click.argument('boot-configuration', type=str, nargs=1)
@click.argument('session-name', type=str, nargs=1)
@click.pass_context
def unified_shell(
    ctx,
    process_manager_configuration:str,
    boot_configuration:str,
    session_name:str,
    log_level:str,
) -> None:
    from drunc.utils.configuration import find_configuration
    ctx.obj.boot_configuration = find_configuration(boot_configuration)
    ctx.obj.session_name = session_name

    # Check if process_manager_configuration is a packaged config
    from urllib.parse import urlparse
    import os
    ## Make the configuration name finding easier
    if os.path.splitext(process_manager_configuration)[1] != '.json':
        process_manager_configuration += '.json'
    ## If no scheme is provided, assume that it is an internal packaged configuration.
    ## First check it's not an existing external file
    if os.path.isfile(process_manager_configuration):
        if urlparse(process_manager_configuration).scheme == '':
            process_manager_configuration = 'file://' + process_manager_configuration
    else:
        ## Check if the file is in the list of packaged configurations
        from importlib.resources import path
        packaged_configurations = os.listdir(path('drunc.data.process_manager', ''))
        if process_manager_configuration in packaged_configurations:
            process_manager_configuration = 'file://' + str(path('drunc.data.process_manager', '')) + '/' + process_manager_configuration
        else:
            from rich import print as rprint
            rprint(f"Configuration [red]{process_manager_configuration}[/red] not found, check filename spelling or use a packaged configuration as one of [green]{packaged_configurations}[/green]")
            exit()
            #from drunc.exceptions import DruncShellException
            #raise DruncShellException(f"Configuration {process_manager_configuration} is not found in the package. The packaged configurations are {packaged_configurations}")

    from drunc.utils.utils import update_log_level, pid_info_str, ignore_sigint_sighandler
    update_log_level(log_level)
    from logging import getLogger
    logger = getLogger('unified_shell')
    logger.debug(pid_info_str())

    from drunc.process_manager.interface.process_manager import run_pm
    import multiprocessing as mp
    ready_event = mp.Event()
    port = mp.Value('i', 0)

    ctx.obj.pm_process = mp.Process(
        target = run_pm,
        kwargs = {
            "pm_conf": process_manager_configuration,
            "log_level": log_level,
            "ready_event": ready_event,
            "signal_handler": ignore_sigint_sighandler,
            # sigint gets sent to the PM, so we need to ignore it, otherwise everytime the user ctrl-c on the shell, the PM goes down
            "generated_port": port,
        },
    )
    ctx.obj.print(f'Starting process manager with configuration {process_manager_configuration}')
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
        ctx.obj.critical(f'Could not connect to the process manager')
        if not ctx.obj.pm_process.is_alive():
            ctx.obj.critical(f'The process manager is dead, exit code {ctx.obj.pm_process.exitcode}')
        raise e

    ctx.obj.info(f'{process_manager_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    if desc.HasField('broadcast'):
        ctx.obj.start_listening_pm(
            broadcaster_conf = desc.broadcast,
        )

    def cleanup():
        ctx.obj.terminate()
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
        describe, status, connect, take_control, surrender_control, who_am_i, who_is_in_charge, fsm, include, exclude, wait
    )

    ctx.command.add_command(describe, 'describe')
    ctx.command.add_command(status, 'status')
    ctx.command.add_command(connect, 'connect')
    ctx.command.add_command(take_control, 'take-control')
    ctx.command.add_command(surrender_control, 'surrender-control')
    ctx.command.add_command(who_am_i, 'whoami')
    ctx.command.add_command(who_is_in_charge, 'who-is-in-charge')
    ctx.command.add_command(include, 'include')
    ctx.command.add_command(exclude, 'exclude')
    ctx.command.add_command(wait, 'wait')
