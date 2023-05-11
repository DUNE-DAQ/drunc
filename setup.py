from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="drunc",
    package_data={
        'drunc': []
    },
    install_requires=[
        "aiostream",
        "click",
        "click_shell",
        "grpcio",
        "grpcio_tools",
        "googleapis-common-protos",
        "grpcio-status",
        "nest_asyncio",
        "rich",
        "sh",
        "druncschema"
    ],
    extras_require={"develop": [
        "ipdb",
        "ipython"
    ]},
)
