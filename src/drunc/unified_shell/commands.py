import getpass
import os
import socket
import sys

import click
import conffwk
import confmodel_dal
from druncschema.process_manager_pb2 import ProcessInstance, ProcessQuery

from drunc.controller.interface.shell_utils import controller_setup
from drunc.exceptions import DruncSetupException
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
    session_resources: list[str] = []
    segments = session_dal.segment.segments
    while segments:
        nested_segments = []
        for segment in segments:
            segment_resources = list(
                confmodel_dal.segment_get_managed_object_tags(
                    db._obj, ctx.obj.configuration_id, segment.id
                )
            )
            managed_objects[segment.id] = segment_resources
            session_resources += segment_resources
            if managed_objects[segment.id]:
                managed_objects_present = True
            nested_segments += [nested_segment for nested_segment in segment.segments]
        segments = nested_segments
    ctx.obj.managed_objects_present = managed_objects_present
    ctx.obj.managed_objects = managed_objects

    # Map the requested dataflow localhost paths to realpaths, and localhost to host names
    for segment, _managed_objects in managed_objects.items():
        log.info(
            f"Segment '{segment}' has requested the following managed objects: {', '.join(_managed_objects)}"
        )
        for i, managed_object in enumerate(_managed_objects):
            # Correct the storage paths if necessary
            if managed_object.startswith("storage:"):
                log.debug(f"Mapping storage path '{managed_object}' to real path")

                # Map localhost to the host name
                if "localhost" in managed_object:
                    updated_host = managed_object.replace(
                        "localhost", socket.gethostname()
                    )
                    _managed_objects[i] = updated_host

                # Map the path to a real path, the paths are commonly "."
                parts = _managed_objects[i].split(":")
                raw_path = parts[-1]
                real_path = os.path.abspath(raw_path)
                mount = "/".join(real_path.split("/")[:2])

                prefix = ":".join(parts[:-1])
                _managed_objects[i] = f"{prefix}:{mount}"
                log.info(
                    f"Mapped storage path '{managed_object}' to real path '{_managed_objects[i]}'"
                )

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

    # Remove storage related ones for initial prototyping
    ctx.obj.session_resources = [
        r for r in session_resources if not r.startswith("storage:")
    ]

    # Query the resources from the resource manager to check for availability
    if ctx.obj.resource_manager_client and ctx.obj.session_resources:
        log.info(
            f"Validating the availability of the requested resources from the resource manager at '{ctx.obj.resource_manager_client.url}': {', '.join(ctx.obj.session_resources)}"
        )

        # Query the resource manager to check if the requested resources are available,
        # and if so, request them. Note that we do this prior to booting any processes,
        # to avoid booting processes and then having the resource manager deny the
        # availability of the requested resources.
        query_resources_response = ctx.obj.resource_manager_client.query_resources(
            ctx.obj.session_resources,
            getpass.getuser(),
            ctx.obj.configuration_id,
            session_name,
        )

        if query_resources_response.get("missing", True):
            log.error(
                f"The resource manager reports that the requested resources are not available. Response: {query_resources_response}"
            )
            return

        # Validate that the requested resources are available in the resource manager
        query_resource_response = ctx.obj.resource_manager_client.query_resources(
            ctx.obj.session_resources,
            getpass.getuser(),
            ctx.obj.configuration_id,
            session_name,
        )
        unavailable_resources = [
            resource.get("name") for resource in query_resource_response.get("query_results", []) 
            if resource.get("session_name") != None
        ]

        # If there are any unavailable resources, log them and block booting, as 
        # the resources required to take the run are unavailable
        if unavailable_resources:
            log.error(f"Resources {unavailable_resources} are not available, blocking run.")
            return
        else:
            log.info(f"Resources {ctx.obj.session_resources} are available.")

        # Allocate the requested resources in the resource manager
        request_resource_response = ctx.obj.resource_manager_client.request_resources(
            ctx.obj.session_resources,
            getpass.getuser(),
            ctx.obj.configuration_id,
            session_name,
        )

        # Check that the allocated resources match the requested resources, if not,
        # log an error and block booting to avoid potential issues with processes
        # booting without the required resources. Note that we check the allocated
        # resources for this session and user, to avoid issues where other
        # sessions/users have requested the same resources. The query checks the 
        # resources against both the session name and user name.
        query_resource_response = ctx.obj.resource_manager_client.query_resources(
            ctx.obj.session_resources,
            getpass.getuser(),
            ctx.obj.configuration_id,
            session_name,
        )
        allocated_resources = [
            resource.get("name") for resource in query_resource_response.get("query_results", []) 
            if resource.get("session_name") == session_name and resource.get("user_name") == getpass.getuser()
        ]
        missing_resources = set(ctx.obj.session_resources) - set(allocated_resources)
        if missing_resources:
            color_coded_missing_resources_str = ", ".join([f"[red]{r}[/red]" for r in missing_resources])
            log.error(
                f"After requesting resources, resources {color_coded_missing_resources_str} have not been allocated, stopping boot. Allocated resources will need to be manually released. "
            )
            log.debug(f"Response: {request_resource_response}")
            return
        else:
            log.info(f"Resources {ctx.obj.session_resources} have been allocated.")

    processes = obj.get_driver("process_manager").ps(
        ProcessQuery(user=user, session=session_name)
    )

    # Store the number of processes that are expected to be booted with this command, to check later if any processes died immediately after booting.
    expected_booted_processes = 0

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
        expected_booted_processes = sum(1 for _ in results)
        for result in results:
            log.critical(
                f"Booting process: {result.values[0].process_description.metadata.name}"
            )
            if not result:
                break
            log.debug(
                f"'{result.values[0].process_description.metadata.name}' ({result.values[0].uuid.uuid}) started"
            )
    except InterruptedCommand:
        log.warning("Booting interrupted")
        return
    except DruncSetupException as e:
        log.error(e)
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

    # If any processes died immediately, place the controller in error.
    alive_process_count = len(
        [p for p in processes.values if p.status_code == ProcessInstance.RUNNING]
    )

    dead_process_count = expected_booted_processes - alive_process_count

    if (
        not obj.get_driver("controller").status().status.in_error
        and dead_process_count == 0
    ):
        log.info("Booted successfully")
    elif dead_process_count != 0:
        log.error(f"Booted, but {dead_process_count} processes died after booting.")
        # The following line has been commented out as there are issues with the k8s PM
        # booting process, which terminates processes and immediately reboots them. The
        # current cause of this issue is unknown, and has been listed in the issue list.
        # obj.get_driver("controller").to_error()
    elif obj.get_driver("controller").status().status.in_error:
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

    # Query the resources from the resource manager to check for availability
    if ctx.obj.resource_manager_client and ctx.obj.session_resources and ctx.obj.managed_objects_present:
        released_resources_str = [f"[green]{r}[/]" for r in ctx.obj.session_resources]

        log.info(
            f"Releasing the requested resources from the resource manager at '{ctx.obj.resource_manager_client.url}': {released_resources_str}"
        )

        # Query the resource manager to check if the requested resources are correctly
        # allocated prior to releasing
        query_resource_response = ctx.obj.resource_manager_client.query_resources(
            ctx.obj.session_resources,
            getpass.getuser(),
            ctx.obj.configuration_id,
            ctx.obj.session_name,
        )
        allocated_resources = [
            resource.get("name") for resource in query_resource_response.get("query_results", []) 
            if resource.get("session_name") == ctx.obj.session_name and resource.get("user_name") == getpass.getuser()
        ]
        missing_resources = set(ctx.obj.session_resources) - set(allocated_resources)
        if missing_resources:
            color_coded_missing_resources_str = ", ".join([f"[red]{r}[/red]" for r in missing_resources])
            log.error(
                f"Upon terrmination, resources {color_coded_missing_resources_str} are not allocated to session {ctx.obj.session_name}, skipping resource release. Allocated resources will need to be manually released."
            )
            log.debug(f"Response: {query_resource_response}")
        else:
            # Release the requested resources from the resource manager
            ctx.obj.resource_manager_client.release_resources(
                ctx.obj.session_resources,
                ctx.obj.configuration_id,
            )
            query_resource_response = ctx.obj.resource_manager_client.query_resources(
                ctx.obj.session_resources,
                getpass.getuser(),
                ctx.obj.configuration_id,
                ctx.obj.session_name,
            )

            # Check that the resources have been released correctly, if not, log an 
            # error
            remaining_session_allocated_resources = [
                resource.get("name") for resource in query_resource_response.get("query_results", []) 
                if resource.get("session_name") == ctx.obj.session_name and resource.get("user_name") == getpass.getuser()
            ]
            if remaining_session_allocated_resources:
                color_coded_remaining_resources_str = ", ".join([f"[red]{r}[/red]" for r in remaining_session_allocated_resources])
                log.critical(f"Resources {color_coded_remaining_resources_str} were not appropriately released, manually release these prior to starting any more runs.")
                ctx.obj.managed_objects = {}
                ctx.obj.managed_objects_present = False
            else:
                log.info(f"Resources {', '.join(released_resources_str)} have been released.")
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
