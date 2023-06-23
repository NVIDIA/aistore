from setuptools import setup

setup(
    name="pyaisloader",
    version="0.0.1",
    entry_points={
        "console_scripts": [
            "pyaisloader=pyaisloader.main:main",
        ],
    },
)
