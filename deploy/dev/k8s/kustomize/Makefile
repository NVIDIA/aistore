.PHONY: minimal minimal-cleanup minimal-https minimal-https-cleanup minimal-cloud minimal-cloud-cleanup ci ci-envs ci-secrets

# Run AIStore cluster consisting of 1 proxy/gateway and 1 target
minimal:
	kubectl apply -k base/common
	kubectl apply -k base/proxy
	kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0
	kubectl apply -k base/target
	kubectl rollout status statefulset/ais-target

minimal-cleanup:
	kubectl delete -k base/common || true
	kubectl delete -k base/proxy || true
	kubectl delete -k base/target || true
	./utils/node_cleanup.sh

# Run AIStore cluster consisting of 1 proxy/gateway and 1 target w/ HTTPS enabled
minimal-https: install-cmctl
	kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.1/cert-manager.yaml
	cmctl check api --wait=2m
	kubectl apply -k overlays/samples/https/common
	kubectl apply -k overlays/samples/https/proxy
	kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0
	kubectl apply -k overlays/samples/https/target
	kubectl rollout status statefulset/ais-target

minimal-https-cleanup:
	kubectl delete -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.1/cert-manager.yaml
	kubectl delete -k overlays/samples/https/common || true
	kubectl delete -k overlays/samples/https/proxy || true
	kubectl delete -k overlays/samples/https/target || true
	./utils/node_cleanup.sh

install-cmctl:
	@if ! command -v cmctl >/dev/null 2>&1; then \
		go install github.com/cert-manager/cmctl/v2@latest; \
	fi

# Run AIStore cluster consisting of 1 proxy/gateway and 1 target w/ AWS and GCP backends
minimal-cloud:
	kubectl apply -k overlays/samples/cloud/common
	kubectl apply -k base/proxy
	kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0
	kubectl apply -k overlays/samples/cloud/target
	kubectl rollout status statefulset/ais-target

minimal-cloud-cleanup:
	kubectl delete -k overlays/samples/cloud/common || true
	kubectl delete -k base/proxy || true
	kubectl delete -k overlays/samples/cloud/target || true
	./utils/node_cleanup.sh

# Run AIStore cluster in CI
ci: ci-envs ci-secrets
	kubectl apply -k overlays/ci/common
	kubectl kustomize overlays/ci/proxy --load-restrictor LoadRestrictionsNone | kubectl apply -f -
	kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0
	kubectl kustomize overlays/ci/target --load-restrictor LoadRestrictionsNone | kubectl apply -f -
	kubectl rollout status statefulset/ais-target --timeout=2m

ci-envs:
	./utils/ci/envs.sh

ci-secrets:
	./utils/ci/secrets/setup.sh

