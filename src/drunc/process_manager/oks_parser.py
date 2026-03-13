import os
from typing import TYPE_CHECKING, Any, Dict, List

import confmodel_dal

from drunc.exceptions import DruncException, DruncSetupException
from drunc.process_manager.configuration import get_commandline_parameters
from drunc.utils.utils import get_logger

if TYPE_CHECKING:
    import conffwk


def get_full_db_path(db_path: str) -> str:
    """
    Find the path of the DB

    Parse through the DUNEDAQ_DB_PATH, look for the passed file. If it exists, return
    the full path. Otherwise, raise an error.

    Raise an error if multiple values match the parameter.

    Args:
        db_path - path to the configuration
    """

    log = get_logger("utils.get_full_db_path")

    # If the path is absolute, return it straight away
    if os.path.isabs(db_path):
        return db_path

    # Get the env var that points to the configuration files. If it doesn't exist, raise
    # an exception
    search_path_str: str = os.environ.get("DUNEDAQ_DB_PATH", None)
    if not search_path_str:
        err_str = "DUNEDAQ_DB_PATH not set, exiting."
        raise DruncSetupException(err_str)

    # Split the contents of DUNEDAQ_DB_PATH, this is expected to contain multiple values
    search_paths: list[str] = search_path_str.split(":")

    # Validate there are entries here
    if not search_paths:
        err_str = "There are no paths to search for DBs, exiting."
        raise DruncSetupException(err_str)

    # Find the matched paths
    matched_configuration_files: list[str] = []

    # Iterate through the search paths to find the configuration file.
    for path in search_paths:
        # Strip out None or empty entries (e.g. due to trailing colons)
        if not path:
            continue

        # Check the DB path
        candidate_path = os.path.join(path, db_path)
        if os.path.exists(candidate_path):
            matched_configuration_files.append(os.path.abspath(candidate_path))
            continue

        # Check the parent of the DB path
        parent_candidate = os.path.join(os.path.dirname(path), db_path)
        if os.path.exists(parent_candidate):
            abs_parent_candidate = os.path.abspath(parent_candidate)
            if abs_parent_candidate not in matched_configuration_files:
                matched_configuration_files.append(abs_parent_candidate)

    # Clean up duplicates (in case of symlinks or trailing slashes)
    unique_matched_files: list[str] = list(set(matched_configuration_files))

    # If no matches are found in DUNEDAQ_DB_PATH or its parents
    if not unique_matched_files:
        err_str = f"No files found in DUNEDAQ_DB_PATH matching {db_path}."
        raise DruncSetupException(err_str)

    # If multiple matches are found, raise an error to avoid ambiguity
    if len(unique_matched_files) > 1:
        matches_str = ", ".join(list(unique_matched_files))
        err_str = (
            f"Ambiguous match: {db_path} found in multiple locations: {matches_str}"
        )
        raise DruncSetupException(err_str)

    # If exactly one match is found, return it
    resolved_path = unique_matched_files[0]
    log.debug(f"Path {db_path} resolved to {resolved_path}")
    return resolved_path


def collect_variables(variables, env_dict: Dict[str, str]) -> None:
    """!Process a dal::Variable object, placing key/value pairs in a dictionary

    @param variables  A Variable/VariableSet object
    @param env_dict   The desitnation dictionary

    """
    for item in variables:
        if item.className() == "VariableSet":
            collect_variables(item.contains, env_dict)
        else:
            if item.className() == "Variable":
                env_dict[item.name] = item.value


class EnvironmentVariableCannotBeSet(DruncException):
    pass


def component_disabled_from_session_dal(
    session_dal_obj: "conffwk.dal.Session", component_id: str
) -> bool:
    """
    Replaces the following without any db dependence
        confmodel_dal.component_disabled(db._obj, session_dal_obj.id, component_id)

    Uses only the Session DAL object (session_dal_obj) and the component UID.

    Semantics:
      - Returns True if the component UID is explicitly present in session_dal_obj.disabled
      - Returns False otherwise

    Notes:
      - This matches the common meaning of 'disabled' in OKS configs as exposed via
        conffwk.dal.Session.disabled.
      - It does *not* attempt to infer disabled state via parent/child propagation
        rules that may exist in confmodel C++ (if any).
    """
    # session_dal_obj.disabled is a list of DAL objects (Resources) disabled in this session
    try:
        disabled = session_dal_obj.disabled
    except Exception:
        # If OKS/DAL isn't available, fall back to "not disabled"
        return False

    # Compare by UID (DAL .id)
    for d in disabled:
        try:
            if d.id == component_id:
                return True
        except Exception:
            # Defensive: if an element doesn't look like a DAL object, ignore it
            continue

    return False


# Recursively process all Segments in given Segment extracting Applications
def collect_apps(
    config_filename: str,
    session_name: str,
    session_dal_obj: "conffwk.dal.Session",
    segment_obj: "conffwk.dal.Segment",
    env: Dict[str, str],
    tree_prefix: List[int] = [
        0,
    ],
) -> List[Dict]:
    """! Recustively collect (daq) application belonging to segment and its subsegments

    @param session_dal_obj  The session the segment belongs to
    @param segment_obj  Segment to collect applications from

    @return The list of dictionaries holding application attributs

    """

    log = get_logger("process_manager.collect_apps")
    # Get default environment from Session
    defenv = env.copy()

    DB_PATH = os.getenv("DUNEDAQ_DB_PATH")
    if DB_PATH is None:
        log.warning("DUNEDAQ_DB_PATH not set in this shell")
    else:
        defenv["DUNEDAQ_DB_PATH"] = DB_PATH

    collect_variables(session_dal_obj.environment, defenv)

    apps = []

    # Add controller for this segment to list of apps
    controller = segment_obj.controller
    rc_env = defenv.copy()
    collect_variables(controller.application_environment, rc_env)
    rc_env["DUNEDAQ_APPLICATION_NAME"] = controller.id
    host = controller.runs_on.runs_on.id

    tree_id_str = ".".join(map(str, tree_prefix))
    apps.append(
        {
            "name": controller.id,
            "type": controller.application_name,
            "args": get_commandline_parameters(
                config_filename=config_filename,
                session_dal=session_dal_obj,
                session_name=session_name,
                obj=controller,
            ),
            "restriction": host,
            "host": host,
            "env": rc_env,
            "tree_id": tree_id_str,
            "log_path": controller.log_path,
        }
    )

    # Recurse over nested segments
    for idx, sub_segment_obj in enumerate(segment_obj.segments):
        log.debug(f"Considering segment {sub_segment_obj.id}")
        if component_disabled_from_session_dal(session_dal_obj, sub_segment_obj.id):
            log.debug(f"Ignoring segment '{sub_segment_obj.id}' as it is disabled")
            continue

        log.debug(f"Collecting apps for segment {sub_segment_obj.id}")
        new_tree_prefix = tree_prefix + [idx]
        try:
            sub_apps = collect_apps(
                session_name=session_name,
                config_filename=config_filename,
                session_dal_obj=session_dal_obj,
                segment_obj=sub_segment_obj,
                env=env,
                tree_prefix=new_tree_prefix,
            )
        except Exception as e:
            log.exception(e)
            raise e
        for app in sub_apps:
            apps.append(app)

    # Get all the enabled applications of this segment
    app_index = 0
    for app in segment_obj.applications:
        log.debug(f"Considering app {app.id}")
        if "Resource" in app.oksTypes():
            enabled = not component_disabled_from_session_dal(session_dal_obj, app.id)
            log.debug(f"{app.id} {enabled=}")
        else:
            enabled = True
            log.debug(f"{app.id} {enabled=}")

        if not enabled:
            log.debug(f"Ignoring disabled app {app.id}")
            continue

        app_env = defenv.copy()

        # Override with any app specific environment from Application
        collect_variables(app.application_environment, app_env)
        app_env["DUNEDAQ_APPLICATION_NAME"] = app.id

        app_tree_id_str = ".".join(map(str, tree_prefix + [app_index]))

        host = app.runs_on.runs_on.id
        args = get_commandline_parameters(
            config_filename=config_filename,
            session_dal=session_dal_obj,
            session_name=session_name,
            obj=app,
        )
        log.debug(f"Collecting app {app.id} with args {args}")

        data_path = get_writer_directory_path(app, log)
        if not data_path:
            log.debug(f"No data path found for app {app.id}")

        apps.append(
            {
                "name": app.id,
                "type": app.application_name,
                "args": args,
                "restriction": host,
                "host": host,
                "env": app_env,
                "tree_id": app_tree_id_str,
                "log_path": app.log_path,
                "data_path": data_path,
            }
        )
        app_index += 1

    return apps


def get_writer_directory_path(app, log) -> str | None:
    # Map known OKS types to their specific writer attribute
    APP_TYPE_TO_WRITER_ATTR = {
        "DFApplication": "data_writers",
        "TPStreamWriterApplication": "tp_writer",
    }

    # Find which app type we are dealing with
    app_types = app.oksTypes()
    writer_attr_name = None

    for known_type, attr_name in APP_TYPE_TO_WRITER_ATTR.items():
        if known_type in app_types:
            writer_attr_name = attr_name
            break

    if not writer_attr_name:
        return None  # Not a known writer application

    writers = getattr(app, writer_attr_name, None)
    if not writers:
        return None

    # Normalise single-object vs list semantics
    if not isinstance(writers, (list, tuple)):
        writers = [writers]

    writer = writers[0]
    params = getattr(writer, "data_store_params", None)
    if params and getattr(params, "directory_path", None):
        directory_path = params.directory_path
        log.info(f"data path for app {app.id}: {directory_path}")
        return directory_path

    return None


def collect_infra_apps(
    session: "conffwk.dal.Session",
    env: Dict[str, str],
    tree_prefix: List[int],
) -> List[Dict[str, Any]]:
    """! Collect infrastructure applications

    @param session  The session

    @return The list of dictionaries holding application attributs

    """
    log = get_logger("process_manager.collect_infra_apps")

    defenv = env
    DB_PATH = os.getenv("DUNEDAQ_DB_PATH")
    if DB_PATH is None:
        log.warning("DUNEDAQ_DB_PATH not set in this shell")
    else:
        defenv["DUNEDAQ_DB_PATH"] = DB_PATH

    collect_variables(session.environment, defenv)

    apps = []

    for app_index, app in enumerate(session.infrastructure_applications):
        # Skip applications that do not define an application name
        # i.e. treat them as "virtual applications"
        # FIXME: modify schema to explicitly introduce non-runnable applications
        if not app.application_name:
            continue
        this_app_tree_prefix = tree_prefix[:-1] + [tree_prefix[-1] + app_index]

        app_env = defenv.copy()
        collect_variables(app.application_environment, app_env)
        app_env["DUNEDAQ_APPLICATION_NAME"] = app.id

        host = app.runs_on.runs_on.id
        apps.append(
            {
                "name": app.id,
                "type": app.application_name,
                "args": app.commandline_parameters,
                "restriction": host,
                "host": host,
                "env": app_env,
                "tree_id": ".".join(map(str, this_app_tree_prefix)),
                "log_path": app.log_path,
            }
        )

    return apps


# Search segment and all contained segments for apps controlled by
# given controller. Return separate lists of apps and sub-controllers
def find_controlled_apps(db, session, mycontroller, segment):
    apps = []
    controllers = []
    if segment.controller.id == mycontroller:
        for app in segment.applications:
            apps.append(app.id)
        for seg in segment.segments:
            if not confmodel_dal.component_disabled(db._obj, session.id, seg.id):
                controllers.append(seg.controller.id)
    else:
        for seg in segment.segments:
            if not confmodel_dal.component_disabled(db._obj, session.id, seg.id):
                aps, controllers = find_controlled_apps(db, session, mycontroller, seg)
                if len(apps) > 0:
                    break
    return apps, controllers
