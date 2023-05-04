echo "Enabling registry for minikube"

# enable registry
minikube addons enable registry

# removing minikube's registry-fwd container if preset
docker kill registry-fwd || true

# map localhost:5000 to the registry of minikube
echo "Mapping localhost:5000 to minikube registry"
docker run --name registry-fwd --rm -d -it --network=host alpine ash -c "apk add socat && socat TCP-LISTEN:5000,reuseaddr,fork TCP:$(minikube ip):5000"
