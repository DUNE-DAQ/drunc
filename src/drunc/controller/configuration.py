from drunc.utils.configuration import ConfHandler
from drunc.controller.children_interface.child_node import ChildNode

class ControllerConfData: # the bastardised OKS
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
    def find_segment(segment, id):
        if segment.controller.id == id:
            return segment

        for child_segment in segment.segments:
            if ControllerConfHandler.find_segment(child_segment, id) is not None:
                return child_segment

        return None

    def _grab_segment_conf_from_controller(self, configuration):
        self.session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)
        this_segment = ControllerConfHandler.find_segment(self.session.segment, self.oks_key.obj_uid)
        if this_segment is None:
            CouldNotFindSegment(self.oks_key.obj_uid)
        return this_segment

    def _post_process_oks(self):
        self.authoriser = None
        self.children = []
        self.data = self._grab_segment_conf_from_controller(self.data)

        self.this_host = self.data.controller.runs_on.runs_on.id
        if self.this_host in ['localhost'] or self.this_host.startswith('127.'):
            import socket
            self.this_host = socket.gethostname()


    def get_children(self, init_token, without_excluded=False, connectivity_service=None):

        enabled_only = not without_excluded

        if self.children != []:
            return self.get_children

        session = None

        try:
            import confmodel
            session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)

        except ImportError as e:
            if enabled_only:
                self.log.error('OKS was not set up, so configuration does not know about include/exclude. All the children nodes will be returned')
                enabled_only=True


        self.log.debug(f'looping over children\n{self.data.segments}')

        for segment in self.data.segments:
            self.log.debug(segment)

            if enabled_only:
                if confmodel.component_disabled(self.db._obj, session.id, segment.id):
                    continue

            from drunc.process_manager.configuration import get_cla
            new_node = ChildNode.get_child(
                cli = get_cla(self.db._obj, session.id, segment.controller),
                init_token = init_token,
                name = segment.controller.id,
                configuration = segment,
                connectivity_service = connectivity_service,
            )
            self.children.append(new_node)

        for app in self.data.applications:
            self.log.debug(app)

            if enabled_only:
                if confmodel.component_disabled(self.db._obj, session.id, app.id):
                    continue

            from drunc.process_manager.configuration import get_cla

            new_node = ChildNode.get_child(
                cli = get_cla(self.db._obj, session.id, app),
                name = app.id,
                configuration = app,
                fsm_configuration = self.data.controller.fsm,
                connectivity_service = connectivity_service,
            )
            self.children.append(new_node)

        return self.children