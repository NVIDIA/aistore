#!/bin/bash

if [ -z "$1" ]
  then
    echo "No argument supplied. Please provide a test type: 'short' or 'long'."
    exit 1
fi

cd ./pyaisloader
make install
export AIS_ENDPOINT="http://localhost:8080"

if [ "$1" == "short" ]
  then
    yes "y" | head -n 2 | pyaisloader p -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -s 1gb -w 16
    yes "y" | head -n 2 | pyaisloader g -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -s 1gb -w 16
    yes "y" | head -n 2 | pyaisloader m -b ais://testpyaisloader -d 15s -min 1mb -max 10mb -w 16 -c
elif [ "$1" == "long" ]
  then
    yes "y" | head -n 2 | pyaisloader p -b ais://testpyaisloader -d 3m -min 1mb -max 10mb -s 10gb -w 16
    yes "y" | head -n 2 | pyaisloader g -b ais://testpyaisloader -d 3m -min 1mb -max 10mb -s 10gb -w 16
    yes "y" | head -n 2 | pyaisloader m -b ais://testpyaisloader -d 3m -min 1mb -max 10mb -w 16 -c
else
    echo "Invalid test type: $1. Please provide a valid test type: 'short' or 'long'."
    exit 1
fi
