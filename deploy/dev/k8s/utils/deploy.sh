#!/bin/bash

# Deploy helper script for AIStore Kustomize deployments

set -e

KUSTOMIZE_PATH="$1"

MANIFEST=$(kubectl kustomize "$KUSTOMIZE_PATH" --load-restrictor LoadRestrictionsNone)

echo -e "$MANIFEST" | kapp deploy -a ais -f- -y --wait-timeout=5m

AIS_ENDPOINT=$(./utils/get_endpoint.sh "$MANIFEST")
EXPORT_CMD="export AIS_ENDPOINT=$AIS_ENDPOINT"
echo "$EXPORT_CMD" > export_endpoint.sh

echo -e "\nTo connect to your cluster, run one of the following:"
echo "  $EXPORT_CMD"
echo "  source export_endpoint.sh"
echo
