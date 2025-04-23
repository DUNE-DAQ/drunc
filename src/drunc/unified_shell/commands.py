import getpass

import click
from drunc_messages.process_manager_pb2 import ProcessQuery

from drunc.controller.interface.shell_utils import controller_setup
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc_core.utils.shell_utils import InterruptedCommand
from drunc_core.utils.utils import get_logger, run_coroutine


@click.command("boot")
@click.option("--override-logs/--no-override-logs", default=True)
@click.pass_obj
@run_coroutine
async def boot(
    obj: ProcessManagerContext,
    override_logs: bool,
) -> None:
    log = get_logger("unified_shell.boot")
    session_name = obj.session_name
    user = getpass.getuser()
    processes = await obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )

    if len(processes.data.values) > 0:
        click.confirm(
            f"You already have {len(processes.data.values)} processes running in session {session_name}, are you sure you want to boot a session?",
            abort=True,
        )

    try:
        results = obj.get_driver("process_manager").boot(
            conf_file=obj.configuration_file,
            conf_id=obj.configuration_id,
            user=user,
            session_name=session_name,
            log_level="INFO",  # Unused anyway !!
            override_logs=override_logs,
        )
        async for result in results:
            if not result:
                break
            log.debug(
                f"'{result.data.process_description.metadata.name}' ({result.data.uuid.uuid}) started"
            )
    except InterruptedCommand:
        log.warning("Booting interrupted")
        return

    controller_address = obj.get_driver("process_manager").controller_address
    if controller_address:
        log.debug(f"Controller endpoint is '{controller_address}'")
        log.debug("Connecting the unified_shell to the controller endpoint")
        obj.set_controller_driver(controller_address)
        controller_setup(obj, controller_address)

    else:
        log.error("Could not understand where the controller is!")
        return

    log.info("Booted successfully")
