import socket
import threading

from kafkaopmon.OpMonPublisher import OpMonPublisher

from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.utils import get_segment_lookup_timeout
from drunc.exceptions import DruncCommandException, DruncSetupException
from drunc.process_manager.configuration import get_commandline_parameters
from drunc.utils.configuration import ConfHandler

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

        if not opmon_uri:
            self.log.info("Missing 'opmon_uri' in configuration.")
            return

        opmon_path = getattr(opmon_uri, "path", "")
        opmon_type = getattr(opmon_uri, "type", "")
        self.opmon_sleep_time = getattr(opmon_uri, "sleep_time", 10.0)

        if not opmon_path or not opmon_type:
            self.log.error("Invalid 'opmon_uri' format: Missing required fields.")
            raise DruncCommandException(
                "Invalid 'opmon_uri' format: Missing required fields."
            )

        self.log.info(
            f"OpMon path {opmon_path} and type {opmon_type} is enabled, sleep time: {self.opmon_sleep_time} s"
        )

        if "/" in opmon_path:
            opmon_bootstrap, opmon_topic = opmon_path.split("/", 1)
        else:
            opmon_bootstrap = opmon_path
            opmon_topic = "opmon_stream"

        if opmon_type == "stream":
            try:
                self.opmon_publisher = OpMonPublisher(
                    default_topic=opmon_topic, bootstrap=opmon_bootstrap
                )
                self.log.info(
                    f"OpMonPublisher initialized: {opmon_bootstrap}/{opmon_topic}"
                )

            except Exception as e:
                self.log.error(f"Failed to initialize OpMonPublisher: {e}")
                raise DruncCommandException("Failed to initialize OpMonPublisher.")
        else:
            self.log.error(f"Unsupported OpMon type: {opmon_type}")
            raise DruncCommandException(f"Unsupported OpMon type: {opmon_type}")

    def get_children(
        self,
        init_token,
        without_excluded=False,
        connectivity_service=None,
        session_name=None,
    ):
        enabled_only = not without_excluded
        timeout = get_segment_lookup_timeout(
            self.data,  # the current segment
            base_timeout=60,
        )

        self.log.debug(f"get_children: connectivity service lookup timeout={timeout}")
        # if self.children != []:
        #    return self.get_children(init_token, without_excluded, connectivity_service)

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
