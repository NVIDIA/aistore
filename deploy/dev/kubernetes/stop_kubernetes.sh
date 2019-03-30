echo "Stopping AIS Clusters"
kubectl delete configmap ais-config
kubectl delete configmap collectd-config
kubectl delete configmap statsd-config

kubectl delete -f aistarget_deployment.yml
if kubectl get statefulset | grep aisproxy > /dev/null 2>&1; then
  kubectl delete -f aisproxy_deployment.yml
fi
kubectl delete -f aisprimaryproxy_deployment.yml

# Remove CLI
rm -f ${GOPATH}/bin/ais