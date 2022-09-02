from drunc.core.configurations import SubControllerConfiguration


def test_subcontrollerconfiguration_json_parsing():
    sc = SubControllerConfiguration.get_from_jsonfile('test_configuration.json')

def test_subcontrollerconfiguration_get_plugin_conf():
    sc = SubControllerConfiguration.get_from_jsonfile('test_configuration.json')
    sc.get_plugin_conf('run_number')
    sc.get_plugin_conf('logging')

def test_subcontrollerconfiguration_get_plugin_list():
    sc = SubControllerConfiguration.get_from_jsonfile('test_configuration.json')
    assert len(sc.get_plugin_list()) == 2
