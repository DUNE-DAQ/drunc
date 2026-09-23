import abc

from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.run_control_pb2_grpc import RunControlServicer
from grpc import ServicerContext

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.utils.utils import get_logger


class RunControl(abc.ABC, RunControlServicer):
    rc_type = "Hehehehe"

    def __init__(self):
        self.log = get_logger("run_control.NAME_run_control")
        self.name = "Some run control thing!"

        self.log.critical("run control active!")

        dach = DummyAuthoriserConfHandler.from_pyobject(
            data=None  # CONFIGURATION NEEDS UPDATING HERE
        )

        self.authoriser = DummyAuthoriser(dach, SystemType.RUN_CONTROL)

        self.commands = [
            CommandDescription(
                name="describe",
                data_type=["None"],
                help="Describe self (return a list of commands, the type of endpoint, the name and session).",
                return_type="description_pb2.Description",
            ),
        ]

    @authentified_and_authorised(action=ActionType.READ, system=SystemType.RUN_CONTROL)
    def describe(self, request: Request, context: ServicerContext) -> Description:
        self.log.info(f"{self.name} running describe")

        response = Description(
            type="some name for now",
            name="unnamed",
            info="no info for now",
            session="no session",
            commands=self.commands,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            token=None,
        )

        return response
