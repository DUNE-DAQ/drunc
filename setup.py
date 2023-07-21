from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="drunc",
    package_data={
        'drunc': []
    },
    install_requires=[
        "click",
        "click_shell",
        "grpcio",
        "googleapis-common-protos",
        "grpcio-status",
        "kafka-python",
        "nest_asyncio",
        "rich",
        "sh"
    ],
    extras_require={"develop": [
        "ipdb",
        "ipython"
    ]},
)
