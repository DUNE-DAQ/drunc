from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="drunc",
    install_requires=[
        "click",
        "click_shell",
        "grpcio",
        "googleapis-common-protos",
        "grpcio_tools",
        "grpcio-status",
        "gunicorn",
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
