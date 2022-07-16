#!/usr/bin/env python3

import os
import sys

from setuptools import setup, find_packages

REQUIRED_MAJOR = 3
REQUIRED_MINOR = 6

# Check for python version
if sys.version_info < (REQUIRED_MAJOR, REQUIRED_MINOR):
    error = ("Your version of python ({major}.{minor}) is too old. You need "
             "python >= {required_major}.{required_minor}.").format(
                 major=sys.version_info.major,
                 minor=sys.version_info.minor,
                 required_minor=REQUIRED_MINOR,
                 required_major=REQUIRED_MAJOR,
             )
    sys.exit(error)

cwd = os.path.dirname(os.path.abspath(__file__))
# Read in README.md for our long_description
with open(os.path.join(cwd, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="aistore",
    version="1.0.1",
    description="A (growing) set of client-side APIs to access and utilize clusters, buckets, and objects on AIStore.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://aiatscale.org',
    download_url='https://github.com/NVIDIA/aistore/tags',
    author='AIStore Team',
    author_email='aistore@exchange.nvidia.com',
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
        "Source": "https://github.com/NVIDIA/aistore/"
    },
    license="MIT",
    python_requires='>=3.6',
    packages=find_packages(exclude=("tests", "tests.*", "examples", "examples.*")),
    install_requires=['requests', 'pydantic==1.9.0'],
    extras_require={
        'pytorch': ['torch', 'torchdata'],
    },
)
