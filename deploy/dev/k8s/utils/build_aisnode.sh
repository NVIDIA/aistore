export MODE="debug"  
DOCKER_IMAGE="aistorage/aisnode-minikube"
IMAGE_TAG="latest"

echo "Building image with mode=${MODE}..."
docker build ./../../../ --force-rm --no-cache -t ${REGISTRY_URL}/${DOCKER_IMAGE}:${IMAGE_TAG} --build-arg MODE="${MODE}" -f Dockerfile
echo "Pushing to ${REGISTRY_URL}/${DOCKER_IMAGE}:${IMAGE_TAG}"
docker push ${REGISTRY_URL}/${DOCKER_IMAGE}:${IMAGE_TAG}