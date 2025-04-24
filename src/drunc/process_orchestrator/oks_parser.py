import os
from typing import Dict, List

import confmodel
from drunc_core.utils.configuration import collect_variables
from drunc_core.utils.utils import get_logger


def collect_infra_apps(session, env: Dict[str, str], tree_prefix) -> List[Dict]:
    """! Collect infrastructure applications

    @param session  The session

    @return The list of dictionaries holding application attributs

    """
    log = get_logger("process_orchestrator.collect_infra_apps")

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
