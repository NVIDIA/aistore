#!/bin/bash

set -e

for path in "$@"; do
  kubectl delete -k "$path" || true
done

source ./utils/node_cleanup.sh