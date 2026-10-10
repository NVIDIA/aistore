#!/bin/bash

set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

go get -u ./...

# clear the upgraded Kubernetes requirements, including kube-openapi
go get \
  k8s.io/api@none \
  k8s.io/apimachinery@none \
  k8s.io/client-go@none \
  k8s.io/metrics@none \
  k8s.io/kube-openapi@none

# let Kubernetes select its required kube-openapi version via MVS
go get \
  k8s.io/api \
  k8s.io/apimachinery \
  k8s.io/client-go \
  k8s.io/metrics

# remove the Go patch version while retaining the selected major/minor
go_version=$(awk '$1 == "go" { split($2, v, "."); print v[1] "." v[2]; exit }' go.mod)
go mod edit -go="$go_version"
go mod tidy
