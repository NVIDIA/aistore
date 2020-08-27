#!/bin/bash

function cleanup() {
  make kill
  make clean
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

# $1 - num_targets; $2 - num_proxies; $3 - num_mountpaths; $4 - cloud
function deploy() {
  cleanup

  echo "build required binaries"
  make cli aisfs aisloader

  targets=$1
  proxies=$2
  { echo $targets; echo $proxies; echo $3; echo $4; echo $5; echo $6; } | MODE="debug" make deploy
  export NUM_PROXY=$proxies
  export NUM_TARGET=$targets
  post_deploy $((targets + proxies))
}

set -o xtrace
source /etc/profile.d/aispaths.sh
source aws.env
source gcs.env

cd $AISSRC && cd ..

git fetch --all

branch="origin/master"
if [[ -n $1 ]]; then
  branch=$1
fi
git reset --hard $branch

git status
git log | head -5

# Setting up minikube for the running kubernetes based tests.
pushd deploy/dev/k8s
# To disable minikube setup comment the following line
{ echo n; } | ./utils/deploy_minikube.sh
popd


# Running kubernetes based tests
export K8S_HOST_NAME="minikube"
# TODO: This requirement can be removed once we do not need single transformer per target.
# We use this because minikube is a 1-node kubernetes cluster
# and with pod anti-affinities (for enabling single transformer per target at a time) it would
# cause failures with pods getting stuck in `Pending` state.
deploy 1 1 3 n n n
BUCKET=test RE="TestKube" make test-run
exit_code=$?
result=$((result + exit_code))
echo "'RE=TestKube make test-run' exit status: $exit_code"

# Deleting minikube cluster
pushd deploy/dev/k8s
./stop.sh
popd

# Running long tests
deploy 6 6 4 y n n
for BUCKET in "aws://ais-jenkins" "gs://ais-nv";do
  BUCKET=$BUCKET make test-long && make test-aisloader
done
exit_code=$?
result=$((result + exit_code))
echo "'make test-long && make test-aisloader' exit status: $exit_code"

cleanup
if [[ $result -ne 0 ]]; then
  echo "tests failed"
fi

exit $result
