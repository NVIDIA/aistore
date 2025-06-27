export LOCAL_DEVELOPMENT ?= false
export CLUSTER_TYPE ?= kind
export CLUSTER_NAME ?= ais-local-deployment

.PHONY: try
try: create-cluster minimal

# Minimal base deployment target
.PHONY: minimal
minimal:
	./utils/deploy.sh base

# Sample overlay targets
.PHONY: https cloud multi-node local-dev external-access
https:
	./utils/deploy.sh overlays/samples/https

cloud:
	./utils/deploy.sh overlays/samples/cloud

multi-node:
	./utils/deploy.sh overlays/samples/multi-node

local-dev:
	./utils/local-development/build_and_load.sh
	./utils/deploy.sh overlays/samples/local-dev

external-access:
	./utils/deploy.sh overlays/samples/external-access

# Cleanup target
.PHONY: cleanup
cleanup:
	kapp delete -a ais --wait-timeout=2m -y
	rm -f export_endpoint.sh

# CI deployment targets
.PHONY: gitlab-ci github-ci
gitlab-ci:
	envsubst < overlays/ci/gitlab/kustomization.tpl.yaml > overlays/ci/gitlab/kustomization.yaml
	./utils/deploy.sh overlays/ci/gitlab

github-ci:
	envsubst < overlays/ci/github/kustomization.tpl.yaml > overlays/ci/github/kustomization.yaml
	./utils/deploy.sh overlays/ci/github

# K8s cluster management targets
.PHONY: create-cluster delete-cluster
create-cluster:
	./utils/manage_cluster.sh create

delete-cluster:
	./utils/manage_cluster.sh delete
