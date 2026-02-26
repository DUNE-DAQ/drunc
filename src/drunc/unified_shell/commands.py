import getpass
import sys

import click
import conffwk
import confmodel_dal
from druncschema.process_manager_pb2 import ProcessQuery

from drunc.controller.interface.shell_utils import controller_setup
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.unified_shell.context import UnifiedShellMode
from drunc.unified_shell.shell_utils import resource_log_tree
from drunc.utils.shell_utils import InterruptedCommand
from drunc.utils.utils import get_logger


@click.command("boot")
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    default=None,
    help="Manual override allows for overwriting logs or not, by appending timestamp info. Default (None) is to follow what is used in the initialisation of the unified shell.",
)
@click.option(
    "--sleep-between-app-boot",
    type=float,
    default=0.1,
    help="Sleep between app boot, in seconds. This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.",
)
@click.pass_obj
@click.pass_context
def boot(
    ctx: click.core.Context,
    obj: ProcessManagerContext,
    override_logs: bool | None,
    sleep_between_app_boot: int | float = 0,
) -> None:
    log = get_logger("unified_shell.boot")

    # Instantiate the session dal to parse out the managed objects
    db = conffwk.Configuration(ctx.obj.configuration_file)
    session_dal = db.get_dal(class_name="Session", uid=ctx.obj.configuration_id)
    session_name = obj.session_name
    user = getpass.getuser()

    # Iterate through all the segment nest levels, parse out the requested managed
    # objects for that segment, and allocate them to a dict
    managed_objects: dict[
        str : list(str)
    ] = {}  # segment: list[managed_object_identifier]
    managed_objects_present: bool = False
    segments = session_dal.segment.segments
    while segments:
        nested_segments = []
        for segment in segments:
            managed_objects[segment.id] = confmodel_dal.segment_get_managed_object_tags(
                db._obj, ctx.obj.configuration_id, segment.id
            )
            if managed_objects[segment.id]:
                managed_objects_present = True
            nested_segments += [nested_segment for nested_segment in segment.segments]
        segments = nested_segments
    ctx.obj.managed_objects_present = managed_objects_present
    ctx.obj.managed_objects = managed_objects

    # Split out the segments that have requested resources
    empty_segments = [k for k, v in managed_objects.items() if not v]
    active_segments = {k: v for k, v in managed_objects.items() if v}

    # Log the request of resources if they are used
    if ctx.obj.managed_objects_present:
        log.info(
            "[blue]Placeholder[/blue] Requesting objects in the following segments:"
        )
    # Note the next 4 lines should be considered to be indented
    if active_segments:
        resource_log_tree(active_segments, log)
    if empty_segments:
        log.info(
            f"[yellow]Empty segments (skipped):[/yellow] {', '.join(empty_segments)}"
        )

    processes = obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )

    if override_logs is None:
        override_logs_boot = obj.override_logs
    else:
        override_logs_boot = override_logs
    if len(processes.values) > 0:
        log.error(
            f"Cannot boot: session {session_name} already has {len(processes.values)} processes running. "
            "Please terminate the existing session first."
        )
        return

    try:
        results = obj.get_driver("process_manager").boot(
            conf_file=obj.configuration_file,
            conf_id=obj.configuration_id,
            user=user,
            session_name=session_name,
            log_level="INFO",  # Unused anyway !!
            override_logs=override_logs_boot,
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


@click.command("terminate")
@click.pass_obj
@click.pass_context
def terminate(ctx, obj):
    """
    Execute the process manager terminate command, but release the resources prior to
    doing so
    """

    log = get_logger("unified_shell.terminate")

    # Get the handle to the managed objects
    all_objects = ctx.obj.managed_objects

    # Split out the segments that have requested resources
    empty_segments = [k for k, v in all_objects.items() if not v]
    active_segments = {k: v for k, v in all_objects.items() if v}

    # Log the release of requested resources if they were used
    if ctx.obj.managed_objects_present:
        log.info(
            "[blue]Placeholder[/blue] Releasing managed objects in the following segments:"
        )

    # if ctx.obj.managed_objects_present:all_objects = ctx.obj.managed_objects
    if active_segments:
        resource_log_tree(active_segments, log)
    if empty_segments:
        log.info(
            f"[yellow]Empty segments (skipped):[/yellow] {', '.join(empty_segments)}"
        )
    ctx.obj.managed_objects = {}
    ctx.obj.managed_objects_present = False

    obj.get_driver("process_manager").terminate()


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
