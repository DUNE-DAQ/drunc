import pytest
from  integtest.tests.drunc_test_factory import drunc_factory
# Get the configuration from the factory

config = drunc_factory(number_of_data_producers=2, run_duration=10)

# Now `config` has everything you need for your test
confgen_arguments = config["confgen_arguments"]
nanorc_command_list = config["nanorc_command_list"]


def test_no_conf_error_in_child_controller(run_nanorc):
    assert run_nanorc.completed_process.returncode == 0
    logs = "".join(log.read_text() for log in run_nanorc.log_files)
    assert "Got error from 'conf' to 'ru-det-conn-0'" not in logs
