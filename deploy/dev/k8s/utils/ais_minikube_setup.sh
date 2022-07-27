echo "Checking kubectl default sa account..."
kubectl get sa default >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
  kubectl create sa default
fi

kubectl apply -f kube_templates/minikube_perms.yaml

# Commands below are run in subshell and the 0 file descriptor is closed
# so they do not "eat" the input.

# Make /var/lib/minikube/ais
(minikube ssh -- 'sudo mkdir -p /var/lib/minikube/ais')

# Mount binding /tmp to a persistent path
(minikube ssh -- 'sudo mount --bind /var/lib/minikube/ais /tmp')

# Create directory for ais-fs
(minikube ssh -- 'sudo mkdir -p /tmp/ais-k8s')
