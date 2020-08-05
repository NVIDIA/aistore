DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"
echo "Building image..."
sudo docker-compose up -d --force-recreate --build --remove-orphans
echo "Pushing to repository..."
export DOCKER_IMAGE="localhost:5000/ais:v1"
docker push ${DOCKER_IMAGE}
