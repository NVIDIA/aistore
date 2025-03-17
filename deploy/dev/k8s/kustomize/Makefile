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

