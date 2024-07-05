import sys

import confmodel
import conffwk

from typing import List, Dict, Any


dal = conffwk.dal.module('x', 'schema/confmodel/dunedaq.schema.xml')

def collect_variables(variables, env_dict: Dict[str, Any]) -> None:
  """!Process a dal::Variable object, placing key/value pairs in a dictionary

  @param variables  A Variable/VariableSet object
  @param env_dict   The desitnation dictionary

  """

  for item in variables:
    if item.className() == 'VariableSet':
      collect_variables(item.contains, env_dict)
    else:
      if item.className() == 'Variable':
        env_dict[item.name] = item.value


# Recursively process all Segments in given Segment extracting Applications
def collect_apps(db, session, segment) -> List[Dict]:
  """
  ! Recustively collect (daq) application belonging to segment and its subsegments

  @param session  The session the segment belongs to
  @param segment  Segment to collect applications from

  @return The list of dictionaries holding application attributs

  """

  import logging
  log = logging.getLogger('collect_apps')
  # Get default environment from Session
  defenv = {}

  import os
  DB_PATH = os.getenv("DUNEDAQ_DB_PATH")
  if DB_PATH is None:
    log.warning("DUNEDAQ_DB_PATH not set in this shell")
  else:
    defenv["DUNEDAQ_DB_PATH"] = DB_PATH

  collect_variables(session.environment, defenv)

  apps = []

  # Add controller for this segment to list of apps
  controller = segment.controller
  appenv = defenv
  collect_variables(controller.application_environment, appenv)
  from drunc.process_manager.configuration import get_cla
  host = controller.runs_on.runs_on.id

  apps.append(
    {
      "name": controller.id,
      "type": controller.application_name,
      "args": get_cla(db._obj, session.id, controller),
      "restriction": host,
      "host": host,
      "env": appenv
    }
  )

  # Recurse over nested segments
  for seg in segment.segments:
    if confmodel.component_disabled(db._obj, session.id, seg.id):
      log.info(f'Ignoring segment \'{seg.id}\' as it is disabled')
      continue

    for app in collect_apps(db, session, seg):
      apps.append(app)

  # Get all the enabled applications of this segment
  for app in segment.applications:
    if 'Component' in app.oksTypes():
      enabled = not confmodel.component_disabled(db._obj, session.id, app.id)
      log.debug(f"{app.id} {enabled=}")
    else:
      enabled = True
      log.debug(f"{app.id} {enabled=}")

    if not enabled:
      log.info(f"Ignoring disabled app {app.id}")
      continue

    appenv = defenv

    # Override with any app specific environment from Application
    collect_variables(app.application_environment, appenv)

    host = app.runs_on.runs_on.id
    apps.append(
      {
        "name": app.id,
        "type": app.application_name,
        "args": get_cla(db._obj, session.id, app),
        "restriction": host,
        "host": host,
        "env": appenv
      }
    )

  return apps


def collect_infra_apps(session) -> List[Dict]:
  """! Collect infrastructure applications 

  @param session  The session

  @return The list of dictionaries holding application attributs
  
  """
  import logging
  log = logging.getLogger('collect_infra_apps')

  defenv = {}

  import os
  DB_PATH = os.getenv("DUNEDAQ_DB_PATH")
  if DB_PATH is None:
    log.warning("DUNEDAQ_DB_PATH not set in this shell")
  else:
    defenv["DUNEDAQ_DB_PATH"] = DB_PATH

  collect_variables(session.environment, defenv)

  apps = []

  for app in session.infrastructure_applications:
    # Skip applications that do not define an application name
    # i.e. treat them as "virtual applications"
    # FIXME: modify schema to explicitly introduce non-runnable applications
    if not app.application_name:
      continue


    appenv = defenv.copy()
    collect_variables(app.application_environment, appenv)


    host = app.runs_on.runs_on.id
    apps.append(
      {
        "name": app.id,
        "type": app.application_name,
        "args": app.commandline_parameters,
        "restriction": host,
        "host": host,
        "env": appenv
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
          break;
  return apps, controllers

