#!/bin/bash

# Install minio test dependencies
pip install -r requirements.txt

# Run our custom test wrapper
python3 run_tests.py
