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
        session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)
        this_segment = ControllerConfHandler.find_segment(session.segment, self.oks_key.obj_uid)
        if this_segment is None:
            CouldNotFindSegment(self.oks_key.obj_uid)
        return this_segment

    def _post_process_oks(self):
        self.authoriser = None
        self.children = []
        self.data = self._grab_segment_conf_from_controller(self.data)

    def get_children(self, init_token, without_excluded=False):

        if self.children != []:
            return self.get_children

        session = None

        try:
            import coredal
            session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)

        except ImportError as e:
            if without_excluded:
                self.log.error('OKS was not set up, so configuration does not know about include/exclude. All the children nodes will be returned')
                without_excluded=True


        self.log.debug(f'looping over children\n{self.data.segments}')

        for segment in self.data.segments:
            self.log.debug(segment)

            if without_excluded:
                if coredal.component_disabled(self.db._obj, session.id, segment.id):
                    continue

            from drunc.process_manager.configuration import get_cla
            new_node = ChildNode.get_child(
                cli = get_cla(self.db._obj, session.id, segment.controller),
                init_token = init_token,
                name = segment.controller.id,
                configuration = segment,
            )
            self.children.append(new_node)

        for app in self.data.applications:
            self.log.debug(app)

            if without_excluded:
                if coredal.component_disabled(self.db._obj, session.id, app.id):
                    continue

            from drunc.process_manager.configuration import get_cla

            new_node = ChildNode.get_child(
                cli = get_cla(self.db._obj, session.id, app),
                name = app.id,
                configuration = app,
                fsm_configuration = self.data.controller.fsm
            )
            self.children.append(new_node)

        return self.children