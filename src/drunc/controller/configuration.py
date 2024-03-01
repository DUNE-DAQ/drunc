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

    def get_children(self):
        return self.children


    def _post_process_oks(self, init_token):
        self.authoriser = None
        self.children = []


        self.log.debug(f'looping over children\n{self.data.segments}')

        for segment in self.data.segments:
            self.log.debug(segment)
            from drunc.controller.children_interface.grpc_child import gRCPChildConfHandler
            new_node = ChildNode.get_child(
                type = ControllerConfHandler.__get_children_type_from_cli(
                    segment.controller.commandline_parameters
                ),
                init_token = init_token,
                name = segment.id,
                configuration = gRCPChildConfHandler(segment, ConfTypes.PyObject),
                fsm_configuration = self.data.controller.fsm # TODO, this should be segment.fsm, at some point
            )
            self.children.append(new_node)

        for app in self.data.applications:
            self.log.debug(app)
            new_node = ChildNode.get_child(
                type = ControllerConfHandler.__get_children_type_from_cli(app.commandline_parameters),
                init_token = init_token,
                name = app.id,
                configuration = app
            )
            self.children.append(new_node)


    @staticmethod
    def __get_children_type_from_cli(CLAs:list[str]) -> ChildNodeType:
        for CLA in CLAs:
            if "rest://" in CLA:
                return ChildNodeType.REST_API
            if "grpc://" in CLA:
                return ChildNodeType.gRPC

        from drunc.exceptions import DruncSetupException
        raise DruncSetupException("Could not find if the child was controlled by gRPC or a REST API")


