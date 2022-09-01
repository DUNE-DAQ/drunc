from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="DRunC",
    package_data={
        'DRunC': []
    },
    install_requires=[],
    extras_require={"develop": [
        "ipdb",
        "ipython"
    ]},
)
