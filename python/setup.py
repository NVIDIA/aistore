#!/usr/bin/env python3

import sys
from pathlib import Path

from setuptools import setup, find_packages

from aistore.sdk.const import UTF_ENCODING

REQUIRED_MAJOR = 3
REQUIRED_MINOR = 6

ROOT_DIR = Path(__file__).parent.resolve()


def _get_version() -> str:
    init_file = ROOT_DIR.joinpath("aistore").joinpath("__init__.py")
    with open(init_file, encoding=UTF_ENCODING) as file:
        for line in file.read().splitlines():
            if line.startswith("__version__"):
                delim = '"' if '"' in line else "'"
                version = line.split(delim)[1]
                print("Setup detected AIStore SDK version:", version)
                return version
    raise RuntimeError(f"Unable to find version string in {init_file}")


# Check for python version
if sys.version_info < (REQUIRED_MAJOR, REQUIRED_MINOR):
    error = (
        "Your version of python ({major}.{minor}) is too old. You need "
        "python >= {required_major}.{required_minor}."
    ).format(
        major=sys.version_info.major,
        minor=sys.version_info.minor,
        required_minor=REQUIRED_MINOR,
        required_major=REQUIRED_MAJOR,
    )
    sys.exit(error)

# Read in README.md for our long_description
with open(ROOT_DIR.joinpath("README.md"), encoding=UTF_ENCODING) as f:
    long_description = f.read()

setup(
    name="aistore",
    version=_get_version(),
    description="A (growing) set of client-side APIs to access and utilize clusters, buckets, and objects on AIStore.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://aiatscale.org",
    download_url="https://github.com/NVIDIA/aistore/tags",
    author="AIStore Team",
    author_email="aistore@exchange.nvidia.com",
    keywords=[
        "AIStore",
        "Artificial Intelligence",
        "Object Storage",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering",
    ],
    project_urls={
        "Documentation": "https://aiatscale.org/docs/",
        "Release notes": "https://github.com/NVIDIA/aistore/releases/",
        "Source": "https://github.com/NVIDIA/aistore/",
    },
    license="MIT",
    python_requires=">=3.6",
    packages=find_packages(exclude=("tests", "tests.*", "examples", "examples.*")),
    # packaging used to check latest version in torchdata
    install_requires=["requests", "packaging", "pydantic==1.9.0", "cloudpickle==2.2.0"],
    extras_require={
        "pytorch": ["torch", "torchdata"],
        "botocore": ["wrapt"],
    },
)
