import abc

from druncschema.run_control_pb2_grpc import RunControlServicer

from drunc.utils.utils import get_logger


class RunControl(abc.ABC, RunControlServicer):
    rc_type = "Hehehehe"

    def __init__(self):
        self.log = get_logger("run_control.NAME_run_control")

        self.log.critical("run control active!")

    @staticmethod
    def get():
        logger = get_logger("run_control.get")
        logger.critical("Hello, world!")
