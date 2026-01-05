
import integrationtest.data_classes as data_classes

def drunc_factory(
    number_of_data_producers=2,
    run_duration=10,
):
    """Factory function to create the configuration for tests."""
    
    # Create the config dictionary or object
    conf_dict = data_classes.drunc_config()
    conf_dict.dro_map_config.n_streams = number_of_data_producers
    conf_dict.op_env = "integtest"
    conf_dict.session = "minimal"
    conf_dict.tpg_enabled = False
    
    substitution = data_classes.attribute_substitution(
        obj_id="random-tc-generator",
        obj_class="RandomTCMakerConf",
        updates={"trigger_rate_hz": 1},
    )
    conf_dict.config_substitutions.append(substitution)
    
    # The core configuration data
    confgen_arguments = {"MinimalSystem": conf_dict}
    
    # The nanorc command list for execution
    nanorc_command_list = (
        "boot conf --target root-controller/ru-controller/ru-det-conn-0 "
        "start --run-number 101 wait 1 enable-triggers wait".split()
        + [str(run_duration)]
        + "conf disable-triggers wait 2 drain-dataflow wait 2 "
          "stop-trigger-sources stop scrap terminate".split()
    )
    
    return {
        "confgen_arguments": confgen_arguments,
        "nanorc_command_list": nanorc_command_list
    }
