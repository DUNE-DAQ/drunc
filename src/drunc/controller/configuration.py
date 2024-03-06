from drunc.utils.configuration import ConfHandler, ConfTypes
from drunc.controller.children_interface.child_node import ChildNode, ChildNodeType

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

    def _post_process_oks(self):
        self.authoriser = None
        self.children = []


    def get_children(self, init_token, without_excluded=False):

        if self.children != []:
            return self.get_children

        if without_excluded:
            try:
                import coredal
                session = self.db.get_dal(class_name="Session", uid=self.oks_key.session)

            except ImportError as e:
                self.log.error('OKS was not set up, so configuration does not know about include/exclude. All the children nodes will be returned')
                without_excluded=True


        self.log.debug(f'looping over children\n{self.data.segments}')

        for segment in self.data.segments:
            self.log.debug(segment)

            if without_excluded:
                if coredal.component_disabled(self.db._obj, session.id, segment.id):
                    continue

            new_node = ChildNode.get_child(
                cli = segment.controller.commandline_parameters,
                init_token = init_token,
                name = segment.id,
                configuration = segment,
            )
            self.children.append(new_node)

        for app in self.data.applications:
            self.log.debug(app)

            if without_excluded:
                if coredal.component_disabled(self.db._obj, session.id, app.id):
                    continue

            new_node = ChildNode.get_child(
                cli = app.commandline_parameters,
                name = app.id,
                configuration = app,
                fsm_configuration = self.data.controller.fsm
            )
            self.children.append(new_node)

        return self.children