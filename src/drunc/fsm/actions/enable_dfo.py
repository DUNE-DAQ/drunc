from drunc.fsm.core import FSMAction
from drunc.fsm.exceptions import EnableDFOFailed
from drunc.utils.configuration import find_configuration


class EnableDFO(FSMAction):
    def __init__(self, configuration):
        super().__init__(name="enable-dfo")
        import logging

        self.log = logging.getLogger("enable-dfo")
        self.conf_dict = {p.name: p.value for p in configuration.parameters}

    def validate_enable_dfo(self, dfo_name, configuration):
        if dfo_name == "":
            # Special case, disable DFO
            return
        # Validate DFO Name
        import conffwk

        db = conffwk.Configuration(f"oksconflibs:{configuration}")
        dfos = db.get_dals(class_name="DFOApplication")
        dfo_found = False
        for dfo in dfos:
            if dfo.id == dfo_name:
                dfo_found = True
                break

        if not dfo_found:
            raise EnableDFOFailed(dfo_name)

    def pre_enable_dfo(self, _input_data, _context, dfo_name: str, **kwargs):
        run_configuration = find_configuration(_context.configuration.initial_data)
        self.validate_enable_dfo(dfo_name, run_configuration)
        _input_data["dfo"] = dfo_name
        return _input_data
