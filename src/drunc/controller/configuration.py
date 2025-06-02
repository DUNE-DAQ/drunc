import socket
import threading

from kafkaopmon.OpMonPublisher import OpMonPublisher as KafkaOpMonPublisher
from opmonlib.publisher import OpMonPublisher
from opmonlib.utils import parse_publisher_conf

from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.children_interface.rest_api_child import (
    RESTAPIChildNodeConfHandler,
)
from drunc.exceptions import DruncCommandException, DruncSetupException
from drunc.process_manager.configuration import get_commandline_parameters
from drunc.utils.configuration import ConfHandler, ConfTypes
from drunc.utils.utils import ControlType

import confmodel  # isort: skip


class ControllerConfData:  # the bastardised OKS
    def __init__(self):
        class id_able:
            id = None

        class cler:
            pass

        self.controller = cler()
        self.controller.broadcaster = id_able()
        self.controller.fsm = id_able()


class ControllerConfHandler(ConfHandler):
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

    def _post_process_oks(self):
        self.authoriser = None
        self.children = []
        self.data = self._grab_segment_conf_from_controller(self.data)

        self.this_host = self.data.controller.runs_on.runs_on.id
        if self.this_host in ["localhost"] or self.this_host.startswith("127."):
            self.this_host = socket.gethostname()

        self.opmon_publisher = None
        opmon_uri = self.session.opmon_uri
        opmon_conf = self.data.controller.opmon_conf

        self.opmon_conf = parse_publisher_conf(self.log, opmon_conf, opmon_uri)
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
                    "%s OpMonPublisher initialized with configuration %s",
                    self.opmon_conf.opmon_type,
                    self.opmon_conf,
                )

        except Exception as e:
            self.log.error("Failed to initialize OpMonPublisher: %s", e)
            raise DruncCommandException("Failed to initialize OpMonPublisher.")
        return

    def get_dummy_children(self):
        ret = []
        session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)

        for seg in self.data.segments:
            if confmodel.component_disabled(self.db._obj, session.id, seg.id):
                continue
            ret.append(
                ChildNode(
                    name=seg.controller.id,
                    configuration=RESTAPIChildNodeConfHandler(seg, ConfTypes.PyObject),
                    node_type=ControlType.Unknown,
                )
            )
        for app in self.data.applications:
            if confmodel.component_disabled(self.db._obj, session.id, app.id):
                continue
            ret.append(
                ChildNode(
                    name=app.id,
                    configuration=RESTAPIChildNodeConfHandler(app, ConfTypes.PyObject),
                    node_type=ControlType.Unknown,
                )
            )
        return ret

    def update_children(
        self,
        children,
        init_token,
        without_excluded=False,
        connectivity_service=None,
        session_name=None,
    ):
        enabled_only = not without_excluded
        timeout = 60  # 60s for each application to start and show up on the connectivity service

        self.log.debug(f"get_children: connectivity service lookup timeout={timeout}")

        session = None
        self.children = []

        try:
            session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)

        except ImportError:
            if enabled_only:
                self.log.error(
                    "OKS was not set up, so configuration does not know about include/exclude. All the children nodes will be returned"
                )
                enabled_only = True

        self.log.debug(f"looping over children\n{self.data.segments}")

        def process_segment(segment):
            if enabled_only:
                if confmodel.component_disabled(self.db._obj, session.id, segment.id):
                    return

            new_node = ChildNode.get_child(
                cli=get_commandline_parameters(
                    db=self.db,
                    config_filename=self.initial_data,
                    session_id=session.id,
                    session_name=session_name,
                    obj=segment.controller,
                ),
                init_token=init_token,
                name=segment.controller.id,
                configuration=segment,
                connectivity_service=connectivity_service,
                timeout=timeout,
            )
            if new_node:
                got_child = False

                for idx, child in enumerate(children):
                    if child.name == new_node.name:
                        children[idx] = new_node
                        got_child = True
                        break
                if not got_child:
                    self.children.append(new_node)

        def process_application(app):
            if enabled_only:
                if confmodel.component_disabled(self.db._obj, session.id, app.id):
                    return

            commandline_parameters = get_commandline_parameters(
                db=self.db,
                config_filename=self.initial_data,
                session_id=session.id,
                session_name=session_name,
                obj=app,
            )

            new_node = ChildNode.get_child(
                cli=commandline_parameters,
                name=app.id,
                configuration=app,
                fsm_configuration=self.data.controller.fsm,
                connectivity_service=connectivity_service,
                timeout=60,
            )
            if new_node:
                got_child = False

                for idx, child in enumerate(children):
                    if child.name == new_node.name:
                        children[idx] = new_node
                        got_child = True
                        break
                if not got_child:
                    self.children.append(new_node)

        # threading the children look up
        threads = []

        for segment in self.data.segments:
            self.log.debug(segment)
            t = threading.Thread(target=process_segment, args=(segment,))
            threads.append(t)
            t.start()

        for app in self.data.applications:
            self.log.debug(app)
            t = threading.Thread(target=process_application, args=(app,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return self.children
