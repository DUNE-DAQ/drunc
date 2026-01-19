import getpass
import sys

import click
from druncschema.process_manager_pb2 import ProcessQuery

from drunc.controller.interface.shell_utils import controller_setup
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.unified_shell.context import UnifiedShellMode
from drunc.utils.shell_utils import InterruptedCommand
from drunc.utils.utils import get_logger


@click.command("boot")
@click.option(
    "--override-logs/--no-override-logs", default=True
)  # see https://click.palletsprojects.com/en/stable/options/
@click.option(
    "--sleep-between-app-boot",
    type=float,
    default=0.1,
    help="Sleep between app boot, in seconds. This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.",
)
# Have it as an optional argument
# Then in the boot object, define a check that sees if you manually override it (takes precedence)
# If not, then check context metadata

@click.pass_obj
def boot(
    obj: ProcessManagerContext,  # In this object you can define an attribute called (there is already a run mode)
    override_logs: bool,
    sleep_between_app_boot: int | float = 0,
) -> None:
    log = get_logger("unified_shell.boot")
    session_name = obj.session_name
    user = getpass.getuser()
    processes = obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )

    #! See if you can print out the processmanagercontext
    log.critical(obj.running_mode)

    #! Now check if you can hook into it..
    log.critical("hooking into override_logs")
    log.critical(obj.override_logs)
    # * YOO THIS TOTALLY WORKS!! OKAY YOU CAN DEVELOP THIS CONVEPT

    #! Now check for the entire tristate area
    # log.critical("Checking overridestate")

    # log.critical(overridestate)
    # log.info(None)

    if len(processes.values) > 0:
        click.confirm(
            f"You already have {len(processes.values)} processes running in session {session_name}, are you sure you want to boot a session?",
            abort=True,
        )
    # test = "50"
    log.critical(f"Override logs boot: {override_logs}")

    #! Pass the keyword argument frmo the unified shell to the key loggs

    #! This is path two
    # This is going to actually boot the actual thing

    try:
        results = obj.get_driver("process_manager").boot(
            conf_file=obj.configuration_file,
            conf_id=obj.configuration_id,
            user=user,
            session_name=session_name,
            log_level="INFO",  # Unused anyway !!
            override_logs=override_logs,
            sleep_between_app_boot=sleep_between_app_boot,
        )
        for result in results:
            if not result:
                break
            log.debug(
                f"'{result.values[0].process_description.metadata.name}' ({result.values[0].uuid.uuid}) started"
            )
    except InterruptedCommand:
        log.warning("Booting interrupted")
        return

    processes = obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )
    if not processes.values:
        log.debug("No processes found after boot - stopping due to previous errors")
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

    if not obj.get_driver("controller").status().status.in_error:
        log.info("Booted successfully")
    else:
        log.error("Booted, but the top controller is in error")
        if obj.running_mode in [UnifiedShellMode.BATCH, UnifiedShellMode.SEMIBATCH]:
            log.error(
                "Unified shell: Running in batch mode, and because error state is detected, exiting."
            )
            sys.exit(1)


@click.command("start-shell")
@click.pass_obj
@click.pass_context
def start_shell(ctx, obj):
    """
    Start an interactive shell session.

    This command stops batch mode and enters an interactive shell state,
    allowing you to execute commands interactively.
    """
    log = get_logger("unified_shell.start_shell")

    obj.running_mode = UnifiedShellMode.SEMIBATCH
    log.info("Switching to interactive mode...")
