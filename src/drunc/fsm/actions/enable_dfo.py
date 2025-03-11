from drunc.fsm.core import FSMAction
from drunc.fsm.exceptions import EnableDFOFailed
from drunc.utils.configuration import find_configuration
from drunc.utils.utils import get_logger


class EnableDFO(FSMAction):
    def __init__(self, configuration):
        super().__init__(name="enable-dfo")

        self.log = get_logger("controller.enable_dfo_action")
        self.conf_dict = {p.name: p.value for p in configuration.parameters}

    def validate_enable_dfo(self, dfo_name, session, configuration):
        self.log.debug(f"Validating dfo-name {dfo_name} in session {session}")
        if dfo_name == "DISABLE":
            # Special case, disable DFO
            return ""
        # Validate DFO Name
        import conffwk

        db = conffwk.Configuration(f"oksconflibs:{configuration}")
        dfos = db.get_dals(class_name="DFOApplication")
        sessionobj = db.get_dal("Session", session)
        disabled = sessionobj.disabled
        for dfo in dfos:
            if dfo not in disabled:
                if dfo_name == "":
                    self.log.info(f"No --dfo-name passed to enable-dfo. Enabling first DFO in session {session}: {dfo.id}")
                    return dfo.id
                if dfo.id == dfo_name:
                    return dfo_name
            else:
                self.log.debug(f"Not considering session-disabled DFO {dfo.id}")

        raise EnableDFOFailed(dfo_name, session)

    def pre_enable_dfo(self, _input_data, _context, dfo_name: str = "", **kwargs):
        run_configuration = find_configuration(_context.configuration.initial_data)
        dfo_name = self.validate_enable_dfo(dfo_name, _context.session, run_configuration)
        self.log.debug(f"Enabling DFO {dfo_name}")
        _input_data["dfo"] = dfo_name
        return _input_data
