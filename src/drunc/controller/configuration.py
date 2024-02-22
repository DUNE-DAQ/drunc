from drunc.utils.configuration_utils import ConfigurationHandler

class ControllerConfiguration(ConfigurationHandler):
    def __init__(self, uid, configuration):
        self.uid = uid
        super().__init__(configuration)

    def _parse_oks(self, oks_conf):
        import oksdbinterfaces
        dal = oksdbinterfaces.dal.module('x', 'schema/coredal/dunedaq.schema.xml')
        db = oksdbinterfaces.Configuration("oksconfig:" + oks_conf)
        return db.get_dal(class_name="RCApplication", uid=self.uid)
