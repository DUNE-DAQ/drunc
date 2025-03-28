import os
from typing import Dict, List

import conffwk
import confmodel

from drunc.exceptions import DruncException
from drunc.process_manager.configuration import get_commandline_parameters
from drunc.utils.utils import get_logger

dal = conffwk.dal.module("x", "schema/confmodel/dunedaq.schema.xml")


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


# Recursively process all Segments in given Segment extracting Applications
def collect_apps(
    config_filename,
    session_name,
    db,
    session_obj,
    segment_obj,
    env: Dict[str, str],
    tree_prefix=[
        0,
    ],
) -> List[Dict]:
    """! Recustively collect (daq) application belonging to segment and its subsegments

    @param session_obj  The session the segment belongs to
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

    collect_variables(session_obj.environment, defenv)

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
                db=db,
                config_filename=config_filename,
                session_id=session_obj.id,
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
        if confmodel.component_disabled(db._obj, session_obj.id, sub_segment_obj.id):
            log.debug(f"Ignoring segment '{sub_segment_obj.id}' as it is disabled")
            continue

        log.debug(f"Collecting apps for segment {sub_segment_obj.id}")
        new_tree_prefix = tree_prefix + [idx]
        try:
            sub_apps = collect_apps(
                session_name=session_name,
                config_filename=config_filename,
                db=db,
                session_obj=session_obj,
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
        if "Component" in app.oksTypes():
            enabled = not confmodel.component_disabled(db._obj, session_obj.id, app.id)
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
            db=db,
            config_filename=config_filename,
            session_id=session_obj.id,
            session_name=session_name,
            obj=app,
        )
        log.debug(f"Collecting app {app.id} with args {args}")

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
            }
        )
        app_index += 1

    return apps


def collect_infra_apps(session, env: Dict[str, str], tree_prefix) -> List[Dict]:
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
            if not confmodel.component_disabled(db._obj, session.id, seg.id):
                controllers.append(seg.controller.id)
    else:
        for seg in segment.segments:
            if not confmodel.component_disabled(db._obj, session.id, seg.id):
                aps, controllers = find_controlled_apps(db, session, mycontroller, seg)
                if len(apps) > 0:
                    break
    return apps, controllers
