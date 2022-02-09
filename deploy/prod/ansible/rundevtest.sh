#!/bin/bash

function cleanup() {
  killall go # Kill test process if still running.
  make kill
  make clean
}

function docker_login() {
  if docker -v ; then
    if [[ -z "$DOCKER_HUB_USER" ]] || [[ -z "$DOCKER_HUB_PASSWORD" ]]; then
      echo "Docker Hub login or password env variable empty"
     else
       docker login -u "$DOCKER_HUB_USER" -p "$DOCKER_HUB_PASSWORD" || echo "Failed to login to Docker Hub"
    fi
  else
    echo "docker binary not found, proceeding without logging in"
  fi
}

function post_deploy() {
  echo "sleep 10 seconds before checking AIStore processes"
  sleep 10

  nodes=$(ps -C aisnode -o pid= | wc -l)
  echo "number of started aisprocs: $nodes"
  if [[ $nodes -lt $1 ]]; then
    echo "some of the aisnodes did not start properly"
    exit 1
  fi
  echo "working with build: $(git rev-parse --short HEAD)"
  echo "run tests with cloud bucket: ${BUCKET}"
}

# $1 - num_targets; $2 - num_proxies; $3 - num_mountpaths; $4 - $7 - cloud; $8 loopback_mpaths
function deploy() {
  cleanup

  echo "build required binaries"
  make cli aisfs aisloader

  targets=$1
  proxies=$2
  { echo $targets; echo $proxies; echo $3; echo $4; echo $5; echo $6; echo $7; echo $8; } | MODE="debug" make deploy
  export NUM_PROXY=$proxies
  export NUM_TARGET=$targets
  post_deploy $((targets + proxies))
}

set -o xtrace
source /etc/profile.d/aispaths.sh
source aws.env
source gcs.env
source run.env

cd $AISSRC && cd ..

git fetch --all

branch=${BRANCH:-"origin/master"}
echo "working on branch ${branch}"
git checkout $branch
git reset --hard $branch

git status
git log | head -5

if [[ "${TEST_ETL,,}" == y* ]]; then
  # Setting up minikube for the running kubernetes based tests.
  pushd deploy/dev/k8s
  echo "Deploying Minikube ========================================================"
  { echo y; echo y; } | ./utils/deploy_minikube.sh

  # Login to Docker Hub to avoid pulls rate limiting in ETL tests.
  docker_login

  echo "Deploying AIS on Minikube"
  target_cnt=5
  proxy_cnt=1
  # NOTE: (4 remote providers + aws cache directory + local registry + datascience stack)
  { echo $target_cnt; echo $proxy_cnt; echo 1; echo 6; echo y; echo n; echo n; echo n; echo "$HOME/aws.env"; echo y; echo n; } | MODE="debug" ./utils/deploy_ais.sh
  echo "AIS on Minikube deployed"
  popd

  kubectl logs -f --max-log-requests $(( target_cnt + proxy_cnt )) -l 'type in (aisproxy,aistarget)' & # Send to background, don't show ETL logs.

  # Running kubernetes based tests
  echo "----- RUNNING K8S TESTS -----"
  AIS_ENDPOINT="$(minikube ip):8080" BUCKET="aws://ais-blossom" RE="TestETL|TestConfig|TestMountpath" make test-run
  exit_code=$?
  result=$((result + exit_code))
  echo "----- K8S TESTS FINISHED WITH: ${exit_code} -----"

  # Deleting minikube cluster
  ./deploy/dev/k8s/stop.sh

  # Clean docker images cache - it takes tens of GBs if not cleaned regularly.
  if docker -v ; then
    docker image prune -a -f
  fi
fi

# Running long tests
deploy ${TARGET_COUNT:-6} ${PROXY_COUNT:-6} ${MPATH_COUNT:-4} ${USE_AWS:-y} ${USE_GCP:-y} ${USE_AZURE:-n} ${USE_HDFS:-n} ${USE_LOOPBACK:-y}
for bucket in ${CLOUD_BCKS}; do
  echo "----- RUNNING LONG TESTS WITH: ${bucket} -----"
  BUCKET=${bucket} make test-long && make test-aisloader
  exit_code=$?
  result=$((result + exit_code))
  echo "----- LONG TESTS FINISHED WITH: ${exit_code} -----"
done

# NOTE: Only the logs from the last make test-long run survive - see function deploy above.
make kill

if [[ $result -ne 0 ]]; then
  echo "tests failed"
fi

exit $result
