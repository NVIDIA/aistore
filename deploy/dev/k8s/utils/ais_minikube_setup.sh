echo "Checking kubectl default sa account..."
kubectl get sa default >/dev/null 2>&1
if [ $? -ne 0 ]; then
  kubectl create sa default
fi

# to allow running kubectl commands from within a pod (for e.g target)
kubectl apply -f kube_templates/minikube_perms.yaml

# making /var/lib/minikube/ais
minikube ssh 'sudo mkdir -p /var/lib/minikube/ais'

# mount binding /tmp to a persistent path
minikube ssh 'sudo mount --bind /var/lib/minikube/ais /tmp'

# creating directory for ais-fs
minikube ssh 'sudo mkdir -p /tmp/ais-k8s'

