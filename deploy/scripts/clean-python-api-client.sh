#!/bin/bash -uex
# Revert and reset the python api-client directory to git head

git clean -fd -- python/api-client
git checkout HEAD -- python/api-client
