echo "Stopping AIS Clusters"
kubectl delete configmap ais-config
kubectl delete configmap collectd-config
kubectl delete configmap statsd-config

kubectl delete -f aistarget_deployment.yml
kubectl delete -f aisprimaryproxy_deployment.yml

