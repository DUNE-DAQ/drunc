import socket
import threading

import confmodel_dal
from daqpytools.logging.handlerconf import HandlerType
from druncschema.token_pb2 import Token
from kafkaopmon.OpMonPublisher import OpMonPublisher as KafkaOpMonPublisher
from opmonlib.publisher import OpMonPublisher
from opmonlib.utils import parse_opmon_conf

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.children_interface.grpc_child import (
    gRCPChildConfHandler,
    gRPCChildNode,
)
from drunc.controller.children_interface.rest_api_child import (
    RESTAPIChildNode,
    RESTAPIChildNodeConfHandler,
)
from drunc.exceptions import DruncCommandException, DruncSetupException
from drunc.process_manager.configuration import get_commandline_parameters
from drunc.utils.configuration import ConfHandler
from drunc.utils.utils import (
    ControlType,
    get_control_type_and_uri_from_cli,
    get_control_type_and_uri_from_connectivity_service,
    get_logger,
)


class ControllerConfHandler(ConfHandler):
    """Handler for controller configuration."""

    @staticmethod
    def find_segment(segment, id_):
        if segment.controller.id == id_:
            return segment

        for child_segment in segment.segments:
            segment = ControllerConfHandler.find_segment(child_segment, id_)
            if segment is not None:
                return segment

        return None

    def _grab_segment_conf_from_controller(self, configuration):
        self.session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)
        this_segment = ControllerConfHandler.find_segment(
            self.session.segment, self.oks_key.obj_uid
        )
        if this_segment is None:
            raise DruncSetupException(
                f"Could not find segment with oks_key.obj_uid: {self.oks_key.obj_uid}"
            )
        return this_segment

    def _post_process_oks(self) -> None:
        self.authoriser = None
        segment = self._grab_segment_conf_from_controller(self._raw_data)
        self.controller = segment.controller
        self.segments = segment.segments
        self.applications = segment.applications

        self.this_host = self.controller.runs_on.runs_on.id
        if self.this_host in ["localhost"] or self.this_host.startswith("127."):
            self.this_host = socket.gethostname()

        self.opmon_publisher = None
        self.opmon_conf = parse_opmon_conf(
            log=self.log,
            conf=self.controller.opmon_conf,
            uri=self.session.opmon_uri,
            session=self.session_name,
            application=self.controller.id,
        )

        if self.opmon_conf.path == "./info.json":
            self.opmon_conf.path = (
                "./info." + self.opmon_conf.session + "." + self.controller.id + ".json"
            )

        self.log.debug("Initializing OpMon with configuration %s", self.opmon_conf)

        try:
            if self.opmon_conf.opmon_type == "stream":
                self.log.debug("Attemtpting to initialize KafkaOpMonPublisher")
                self.opmon_publisher = KafkaOpMonPublisher(self.opmon_conf)
                self.log.debug(
                    "KafkaOpMonPublisher initialized with configuration %s",
                    self.opmon_conf,
                )
            else:
                self.log.debug("Attemtpting to initialize OpMonPublisher")
                self.opmon_publisher = OpMonPublisher(
                    conf=self.opmon_conf, log_level=self.log.getEffectiveLevel()
                )
                self.log.debug(
                    "OpMonPublisher initialized with configuration %s", self.opmon_conf
                )

        except Exception as e:
            self.log.error("Failed to initialize OpMonPublisher: %s", e)
            raise DruncCommandException("Failed to initialize OpMonPublisher.")
        return

    def init_children(
        self,
        session_name: str,
        init_token: Token,
        connectivity_service: ConnectivityServiceClient | None = None,
        enabled_only: bool = True,
    ) -> list[ChildNode]:
        child_nodes: list[ChildNode] = []
        booting_errors: list[Exception] = []

        # 60s for applications to show on the connectivity service.
        timeout = 60
        self.log.debug(f"init_children: connectivity service timeout: {timeout}")

        try:
            session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)
        except ImportError:
            session = None
            if enabled_only:
                self.log.error(
                    "OKS was not set up, so configuration does not know about include/exclude. All the children nodes will be returned"
                )
                enabled_only = False

        def process_segment(segment):
            if enabled_only and confmodel_dal.entity_excluded(
                self.db._obj, session.id, segment.id
            ):
                return  # Ignore excluded segments.

            cmd_args = get_commandline_parameters(
                config_filename=self.initial_data,
                session_dal=session,
                session_name=session_name,
                obj=segment.controller,
            )
            node = self.child_node_factory(
                cmd_args=cmd_args,
                init_token=init_token,
                name=segment.controller.id,
                configuration=segment,
                connectivity_service=connectivity_service,
                timeout=timeout,
            )
            child_nodes.append(node)

        def process_application(app):
            if enabled_only and confmodel_dal.entity_excluded(
                self.db._obj, session.id, app.id
            ):
                return

            try:
                cmd_args = get_commandline_parameters(
                    config_filename=self.initial_data,
                    session_dal=session,
                    session_name=session_name,
                    obj=app,
                )
                node = self.child_node_factory(
                    cmd_args=cmd_args,
                    name=app.id,
                    configuration=app,
                    connectivity_service=connectivity_service,
                    timeout=timeout,
                )
                child_nodes.append(node)
            except ApplicationLookupUnsuccessful as e:
                self.log.warning(
                    f"Application '{app.id}' lookup failed.",
                    extra={"handlers": [HandlerType.Lstdout]},
                )
                booting_errors.append(e)

        # threading the children look up
        threads = []

        for segment in self.segments:
            self.log.debug(segment)
            t = threading.Thread(target=process_segment, args=(segment,))
            threads.append(t)
            t.start()

        for app in self.applications:
            self.log.debug(app)
            t = threading.Thread(target=process_application, args=(app,))
            threads.append(t)
            t.start()

        # Wait for everyone to finish
        for t in threads:
            t.join()

        # Check if any thread reported an error
        if booting_errors:
            self.log.error(
                f"Failed to boot: {booting_errors}",
                extra={"handlers": [HandlerType.Lstdout]},
            )
            raise booting_errors[0]

        return child_nodes

    def child_node_factory(
        self,
        name: str,
        cmd_args: list[str],
        configuration,
        init_token: Token | None = None,
        connectivity_service: ConnectivityServiceClient | None = None,
        timeout: int = 60,
    ) -> ChildNode:
        log = get_logger("controller.core.child_node_factory")

        if connectivity_service is not None:
            try:
                # Query the connectivity service.
                ctype, uri = get_control_type_and_uri_from_connectivity_service(
                    connectivity_service, name, timeout=timeout
                )
            except ApplicationLookupUnsuccessful as e:
                log.error(f"Could not find '{name}' in the connectivity service: {e}")
                raise e
        else:
            try:
                # Fall back to the command line arguments.
                ctype, uri = get_control_type_and_uri_from_cli(cmd_args)
            except DruncSetupException as e:
                log.error(f"Could not get '{name}' protocol from CLI: {e}")
                raise e

        log.info(f"Child '{name}' is of type '{ctype}' and has the URI '{uri}'")

        match ctype:
            case ControlType.gRPC:
                grpc_conf_handler = gRCPChildConfHandler.from_pyobject(
                    data=configuration
                )
                return gRPCChildNode(
                    name, grpc_conf_handler, uri, connectivity_service, init_token
                )

            case ControlType.REST_API:
                restapi_conf_handler = RESTAPIChildNodeConfHandler.from_pyobject(
                    data=configuration
                )
                return RESTAPIChildNode(
                    name,
                    restapi_conf_handler,
                    uri,
                    self.controller.fsm,
                    connectivity_service=connectivity_service,
                )

            case _:
                error_message = f"Unknown protocol '{ctype}' for child '{name}'"
                log.error(error_message)
                raise DruncSetupException(error_message)
