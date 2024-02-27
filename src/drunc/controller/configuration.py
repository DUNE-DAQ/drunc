from drunc.utils.configuration_utils import ConfigurationHandler, ConfTypes, ConfData
from drunc.exceptions import DruncSetupException
from drunc.controller.children_interface.child_node import ChildNode, ChildNodeType

class ControllerConfiguration(ConfigurationHandler):
    def __init__(self, uid, configuration):
        self.uid = uid
        super().__init__(configuration)

    def _parse_oks(self, oks_conf):
        import oksdbinterfaces
        dal = oksdbinterfaces.dal.module('x', 'schema/coredal/dunedaq.schema.xml')
        db = oksdbinterfaces.Configuration("oksconfig:" + oks_conf)
        return db.get_dal(class_name="Segment", uid=self.uid)


    def get_broadcast_configuration(self):
        data = None
        match self.conf.type:
            case ConfTypes.OKSObject:
                data = self.conf.data.controller.broadcaster
            case ConfTypes.PyObject:
                data = self.conf.data['broadcaster']
        return ConfData(
            type = self.conf.type,
            data = data
        )

    def get_fsm_configuration(self):
        data = None
        match self.conf.type:
            case ConfTypes.OKSObject:
                data = self.conf.data.controller.fsm
            case ConfTypes.PyObject:
                data = self.conf.data.fsm
        return ConfData(
            type = self.conf.type,
            data = data
        )


    def get_children(self):
        if self.conf.type not in [ConfTypes.PyObject, ConfTypes.OKSObject]:
            raise DruncSetupException('Controller Configuration was not parsed correctly')


        match self.conf.type:
            case ConfTypes.OKSObject:
                return self.__get_children_from_oksobj()
            case ConfTypes.PyObject:
                return self.__get_children_from_pyobj()


    def __get_children_from_pyobj(self):
        children = []

        for child in self.conf.data.children:
            if child.type == 'rest-api': # already some hacking
                new_node = ChildNode.get_child(
                    name = child.name,
                    configuration = child,
                    fsm_configuration = self.configuration.fsm
                )

            else:
                new_node = ChildNode.get_child(
                    name = child['name'],
                    configuration = child,
                )
            children.append(new_node)

        return children

    @staticmethod
    def __get_children_type_from_cli(CLAs:list[str]) -> ChildNodeType:
        for CLA in CLAs:
            if "rest://" in CLA:
                return ChildNodeType.REST_API
            if "grpc://" in CLA:
                return ChildNodeType.gRPC

        from drunc.exceptions import DruncSetupException
        raise DruncSetupException("Could not find if the child was controlled by gRPC or a REST API")


    def __get_children_from_oksobj(self):
        children = []
        self.log.debug(f'looping over children\n{self.conf.data.segments}')

        for segment in self.conf.data.segments:
            self.log.debug(segment)
            new_node = ChildNode.get_child(
                type = ControllerConfiguration.__get_children_type_from_cli(
                    segment.controller.commandline_parameters
                ),
                name = segment.id,
                configuration = ConfData(segment, self.conf.type),
                fsm_configuration = ConfData(self.conf.data.controller.fsm, self.conf.type), # TODO, this should be segment.fsm, at some point
            )
            children.append(new_node)

        for app in self.conf.data.applications:
            self.log.debug(app)
            new_node = ChildNode.get_child(
                type = ControllerConfiguration.__get_children_type_from_cli(app.commandline_parameters),
                name = app.id,
                configuration = ConfData(app, self.conf.type),
            )
            children.append(new_node)

        return children


