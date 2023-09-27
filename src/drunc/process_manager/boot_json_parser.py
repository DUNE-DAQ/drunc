

def process_env(env, rte):
    new_env = {}

    exclude = [
        "CET_PLUGIN_PATH",
        "DUNEDAQ_SHARE_PATH",
        "LD_LIBRARY_PATH",
        "PYTHONPATH",
        "PATH"
    ] if rte else []

    import os

    for name, value in env.items():
        if name in exclude:
            continue
        if type(value) is not str:
            new_env[name] = str(value)
            continue
        if value.startswith('getenv'):
            if name in os.environ:
                value = os.environ[name]
            else:
                if value == 'getenv_ifset':
                    continue
                else:
                    value = value.removeprefix('getenv')
                    value = value.removeprefix(':')
        new_env[name] = value

    return new_env


def process_args(args, env):
    new_args = []
    for arg in args:
        if type(arg) is str:
            new_args += [arg.format(**env)]
        else:
            new_args += [arg]
    return new_args


def process_exec(name, data, env, exec, hosts, **kwargs):
    from druncschema.process_manager_pb2 import BootRequest, ProcessDescription, ProcessRestriction

    from logging import getLogger
    _log = getLogger('process_exec')
    from copy import deepcopy as dc
    exec = dc(exec)
    app_exec = exec[data['exec']]
    app_env = app_exec['env']
    app_env.update(env)
    app_env.update({
        'APP_PORT': data['port'],
        'APP_NAME': name,
        'APP_HOST': hosts[name],
    })

    if 'session' in kwargs:
        app_env['DUNEDAQ_PARTITION'] = kwargs['session']

    if 'conf' in kwargs:
        app_env['CONF_LOC'] = kwargs['conf']

    if 'TRACE_FILE' in app_env:
        app_env.update({
            'TRACE_FILE': app_env['TRACE_FILE'].format(**app_env),
        })
    if 'CMD_FAC' in app_env:
        app_env.update({
            'CMD_FAC': app_env['CMD_FAC'].format(**app_env),
        })

    if 'INFO_SVC' in app_env:
        app_env.update({
            'INFO_SVC': app_env['INFO_SVC'].format(**app_env)
        })

    exec_and_args = []

    rte_present = False
    if 'rte' in kwargs:
        exec_and_args += [ProcessDescription.ExecAndArgs(
                exec = 'source',
                args = [kwargs['rte']]
        )]
        rte_present = True

    app_env = process_env(
        env = app_env,
        rte = rte_present
    )
    _log.debug(app_env)


    exec_and_args += [ProcessDescription.ExecAndArgs(
        exec = app_exec['cmd'],
        args = process_args(app_exec['args'], app_env)
    )]

    pd = ProcessDescription(
        executable_and_arguments = exec_and_args,
        env = app_env
    )

    pr = ProcessRestriction(
        allowed_hosts = [hosts[name]]
    )

    return BootRequest(
        process_description = pd,
        process_restriction = pr,
    )


def parse_configuration(input_dir, output_dir):
    import os
    os.mkdir(output_dir)

    import json

    boot_configuration = {}

    with open(input_dir/'boot.json') as f:
        boot_configuration = json.loads(f.read())

    for filename in os.listdir(input_dir/'data'):
        with open(input_dir/'data'/filename) as f:
            data = json.loads(f.read())
            if not 'connections' in data:
                json.dump(data, open(output_dir/filename,'w'))
                continue

            new_connections = []
            for c in data['connections']:
                nc = c
                nc['uri'] = nc['uri'].format(**boot_configuration['hosts-data'])
                new_connections += [nc]
            data['connections'] = new_connections

            json.dump(data, open(output_dir/filename,'w'), indent=4)

