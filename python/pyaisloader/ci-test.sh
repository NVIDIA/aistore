#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "No argument supplied. Please provide a test type: 'short', 'long', or 'etl'."
    exit 1
fi

# Install aistore python SDK from source
pip install -e . --quiet

cd ./pyaisloader || exit
make install

if [ "$1" == "short" ]; then
    yes "y" | head -n 2 | pyaisloader p -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -s 1gb -w 16
    yes "y" | head -n 2 | pyaisloader g -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -s 1gb -w 16
    yes "y" | head -n 2 | pyaisloader m -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -w 16 -c
    yes "y" | head -n 2 | pyaisloader ais_dataset -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -w 16 -c
    yes "y" | head -n 2 | pyaisloader ais_iter_dataset -b ais://testpyaisloader -i 1 -d 15s -min 1mb -max 10mb -w 16 -c
elif [ "$1" == "long" ]; then
    yes "y" | head -n 2 | pyaisloader p -b ais://testpyaisloader -d 3m -min 1mb -max 10mb -s 10gb -w 16
    yes "y" | head -n 2 | pyaisloader g -b ais://testpyaisloader -d 3m -min 1mb -max 10mb -s 10gb -w 16
    yes "y" | head -n 2 | pyaisloader m -b ais://testpyaisloader -d 3m -min 1mb -max 10mb -w 16 -c
elif [ "$1" == "etl" ]; then
    yes "y" | head -n 2 | pyaisloader m -b ais://testpyaisloader -d 1m -min 1mb -max 10mb -w 16 --etl ECHO
    yes "y" | head -n 2 | pyaisloader m -b ais://testpyaisloader -d 1m -min 1mb -max 10mb -w 16 --etl MD5 -c
else
    echo "Invalid test type: $1. Please provide a valid test type: 'short', 'long', or 'etl'."
    exit 1
fi
