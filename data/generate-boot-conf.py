#!/usr/bin/env python3
import json
from rich.pretty import pprint as print

def get_controller_instance(name, port):
    return {
        "name": name,
        "port": port,
        "restriction": "localhost",
        "type": "drunc-controller",
        "configuration": f"file://data/{name}-conf.json",
    }

def get_daq_app_instance(name, port):
    return {
        "name": name,
        "port": port,
        "restriction": "localhost",
        "type": "fake-daq-application",
        "configuration": f"file://data/{name}-conf.json",
    }


instances = [get_controller_instance(f'topcontroller', 3600)]

import random
random.seed(10)
nsystem = 1

for i in range(nsystem):
    instances.append(get_controller_instance(f'controller{i}', 3601+i))

    napps = random.randint(1,7)
    instances += [get_daq_app_instance(f'app{i}{i2}', 7200+i*1000+i2) for i2 in range(napps)]


executables = {
    "drunc-controller": {
        "executable_and_arguments": [
            {
                "env":[]
            },
            {
                "source": [
                    "${DRUNC_DIR}/setup.sh"
                ]
            },
            {
                "cd" : [
                    "${DRUNC_DIR}"
                ]
            },
            {
                "echo":[
                    "${PATH}"
                ]
            },
            {
                "drunc-controller" : [
                    "${CONFIGURATION}",
                    "${PORT}",
                    "${NAME}"
                ]
            }
        ],
        "environment": {
            "CONFIGURATION": "{configuration}",
            "DRUNC_DIR": "getenv",
            "NAME": "{name}",
            "PORT": "{port}"
        }
    },
    "fake-daq-application": {
        "executable_and_arguments": [
            {
                "env":[]
            },
            {
                "source": [
                    "${DRUNC_DIR}/setup.sh"
                ]
            },
            {
                "cd" : [
                    "${DRUNC_DIR}"
                ]
            },
            {
                "echo":[
                    "${PATH}"
                ]
            },
            {
                "fake_daq_application" : [
                    "-n", "${NAME}",
                    "-d", "${CONFIGURATION}",
                    "-c", "rest://localhost:${PORT}",
                ]
            }
        ],
        "environment": {
            "CONFIGURATION": "{configuration}",
            "DRUNC_DIR": "getenv",
            "NAME": "{name}",
            "PORT": "{port}",
        }
    }
}

restrictions = {
    "localhost": {
        "hosts": ["localhost"]
    }
}

boot_data = {
    "instances": instances,
    "executables": executables,
    "restrictions": restrictions,
}

with open('controller-boot-many.json', 'w') as f:
    json.dump(boot_data,f, indent=4)


level0 = ['topcontroller']
level1 = []
level2 = {}

for instance in boot_data['instances']:
    print(instance)
    if   'topcontroller' in instance['name']: continue
    elif 'controller'    in instance['name']:
        level1.append({
            'name': instance['name'],
            'cmd_address': f'localhost:{instance["port"]}'
        })
    elif 'app'in instance['name']:
        controller_name = f'controller{instance["name"][3]}'
        if controller_name in level2:
            level2[controller_name].append({
                'name': instance['name'],
                'cmd_address': f'localhost:{instance["port"]}'
            })
        else:
            level2[controller_name] = [{
                'name': instance['name'],
                'cmd_address': f'localhost:{instance["port"]}'
            }]

print(level1)

print('')
print('')
print('')
level2 = {}

print(level2)
def generate_conf_data(controller_name:str, children_controller:list, children_app:list) -> None:
    with open(f'{controller_name}-conf.json', 'w') as f:
        json.dump (
            {
                'children_controllers': children_controller,
                'apps': children_app,
            },
            f,
            indent=4,
        )


generate_conf_data(
    controller_name = 'topcontroller',
    children_controller = level1,
    children_app = []
)

for subcontroller in level1:
    generate_conf_data(
        controller_name = subcontroller['name'],
        children_controller = [],
        children_app = level2.get(subcontroller['name'], [])
    )

    for app in level2.get(subcontroller['name'], []):
        print(app)

        generate_conf_data(
            controller_name = app,
            children_controller = [],
            children_app = []
        )
