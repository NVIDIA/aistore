#!/bin/bash

echo "Clean deployment"
make kill
make clean
make rmcache
make deploy <<< $'5\n5\n5\n0'
