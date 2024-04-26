import sys

import coredal
import oksdbinterfaces


dal = oksdbinterfaces.dal.module('x', 'schema/coredal/dunedaq.schema.xml')

# Process a dal::Variable object, placing key/value pairs in a dictionary
def process_variables(variables, envDict):
  for item in variables:
    if item.className() == 'VariableSet':
      process_variables(item.contains, envDict)
    else:
      if item.className() == 'Variable':
        envDict[item.name] = item.value

# Recursively process all Segments in given Segment extracting Applications
def process_segment(db, session, segment):
  import logging
  log = logging.getLogger('process_segment')
  # Get default environment from Session
  defenv = {}

  import os
  DB_PATH = os.getenv("DUNEDAQ_DB_PATH")
  if DB_PATH is None:
    log.warning("DUNEDAQ_DB_PATH not set in this shell")
  else:
    defenv["DUNEDAQ_DB_PATH"] = DB_PATH

  process_variables(session.environment, defenv)

  apps = []

  # Add controller for this segment to list of apps
  controller = segment.controller
  appenv = defenv
  process_variables(controller.applicationEnvironment, appenv)
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
    if coredal.component_disabled(db._obj, session.id, seg.id):
      log.info(f'Ignoring segment \'{seg.id}\' as it is disabled')

    for app in process_segment(db, session, seg):
      apps.append(app)

  # Get all the enabled applications of this segment
  for app in segment.applications:
    if 'Component' in app.oksTypes():
      enabled = not coredal.component_disabled(db._obj, session.id, app.id)
      log.debug(f"{app.id} {enabled=}")
    else:
      enabled = True
      log.debug(f"{app.id} {enabled=}")

    if not enabled:
      log.info(f"Ignoring disabled app {app.id}")

    appenv = defenv

    # Override with any app specific environment from Application
    process_variables(app.applicationEnvironment, appenv)

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

def process_services(session):
  services = []
  for srv in session.services:
    if isinstance(srv, dal.Application) and srv.enabled:
      services.append((srv.className(), srv.runs_on))
  return services


# Search segment and all contained segments for apps controlled by
# given controller. Return separate lists of apps and sub-controllers
def find_controlled_apps(db, session, mycontroller, segment):
  apps = []
  controllers = []
  if segment.controller.id == mycontroller:
    for app in segment.applications:
      apps.append(app.id)
    for seg in segment.segments:
      if not coredal.component_disabled(db._obj, session.id, seg.id):
        controllers.append(seg.controller.id)
  else:
    for seg in segment.segments:
      if not coredal.component_disabled(db._obj, session.id, seg.id):
        aps, controllers = find_controlled_apps(db, session, mycontroller, seg)
        if len(apps) > 0:
          break;
  return apps, controllers

