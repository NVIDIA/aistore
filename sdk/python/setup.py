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
    version="0.1.0",
    description="Client and convenient connectors for PyTorch and TensorFlow to AIStore cluster",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://aiatscale.org',
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
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering",
    ],
    license="MIT",
    python_requires='>=3.6',
    packages=find_packages(exclude=("tests", "tests.*")),
    install_requires=['requests', 'cloudpickle==2.0.0', 'pydantic==1.9.0'],
    extras_require={
        'pytorch': ['torch', 'torchvision'],
        'tf': ['braceexpand', 'humanfriendly', 'tensorflow', 'tensorflow_addons'],
    },
)
