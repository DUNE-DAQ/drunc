from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="drunc",
    package_data={
        'drunc': []
    },
    install_requires=[],
    extras_require={"develop": [
        "ipdb",
        "ipython"
    ]},
)
