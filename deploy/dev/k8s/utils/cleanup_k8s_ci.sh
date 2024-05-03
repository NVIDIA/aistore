# Kill all pods and services created by ci job and ignore errors
kubectl delete pods -l app=ais,type=ais-target || true
kubectl delete pods -l app=ais,type=ais-proxy || true
kubectl delete svc -l app=ais || true