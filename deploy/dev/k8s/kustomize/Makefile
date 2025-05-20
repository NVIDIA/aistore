export LOCAL_DEVELOPMENT ?= false
export CLUSTER_TYPE ?= kind

# Quick start target
.PHONY: try
try: create-cluster minimal

# Minimal deployment targets
.PHONY: minimal minimal-cleanup
minimal:
	./utils/deploy_ais.sh base/common base/proxy base/target

minimal-cleanup:
	./utils/cleanup_ais.sh base/common base/proxy base/target

# Sample HTTPs deployment targets
.PHONY: minimal-https minimal-https-cleanup install-cmctl
minimal-https: install-cmctl
	kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.1/cert-manager.yaml
	cmctl check api --wait=2m
	./utils/deploy_ais.sh overlays/samples/https/common overlays/samples/https/proxy overlays/samples/https/target

minimal-https-cleanup:
	kubectl delete -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.1/cert-manager.yaml
	./utils/cleanup_ais.sh overlays/samples/https/common overlays/samples/https/proxy overlays/samples/https/target

install-cmctl:
	@if ! command -v cmctl >/dev/null 2>&1; then \
		go install github.com/cert-manager/cmctl/v2@latest; \
	fi

# Sample cloud deployment targets
.PHONY: minimal-cloud minimal-cloud-cleanup
minimal-cloud:
	./utils/deploy_ais.sh overlays/samples/cloud/common base/proxy overlays/samples/cloud/target

minimal-cloud-cleanup:
	./utils/cleanup_ais.sh overlays/samples/cloud/common base/proxy overlays/samples/cloud/target

# CI deployment targets
.PHONY: gitlab-ci github-ci ci-envs ci-backend-secrets ci-dockerhub-secrets
gitlab-ci: ci-envs ci-backend-secrets ci-dockerhub-secrets
	./utils/deploy_ais.sh overlays/ci/common overlays/ci/proxy overlays/ci/target

github-ci: ci-dockerhub-secrets minimal

ci-envs:
	./utils/ci/envs.sh

ci-backend-secrets:
	./utils/ci/secrets/backends.sh

ci-dockerhub-secrets:
	./utils/ci/secrets/dockerhub.sh

# K8s cluster management targets
.PHONY: create-cluster delete-cluster
create-cluster:
	./utils/create_cluster.sh

delete-cluster:
	./utils/delete_cluster.sh

# AIStore utility targets
.PHONY: get-endpoint
get-endpoint:
	@. ./utils/export_endpoint.sh && echo "To connect to the cluster, use: AIS_ENDPOINT=$$AIS_ENDPOINT"
