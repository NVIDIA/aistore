#!/bin/bash

function cleanup() {
  make kill
  make clean
  pushd deploy/dev/k8s
  ./stop.sh
  popd
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
  export BUCKET=nvais
  echo "run tests with cloud bucket: ${BUCKET}"
}

set -o xtrace
source /etc/profile.d/aispaths.sh
source aws.env

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
cleanup
export K8S_HOST_NAME="minikube"
# TODO: This requiremenet can be removed once we do not need single transformer per target.
# We use this because minikube is a 1-node kubernetes cluster
# and with pod anti-affinities (for enabling single transformer per target at a time) it would
# cause failures with pods getting stuck in `Pending` state.
num_targets=1
num_proxies=1
{ echo $num_targets; echo $num_proxies; echo 3; echo 0; } | MODE="debug" make deploy
post_deploy $((num_proxies + num_targets))
RE="TestKube" make test-run
exit_code=$?
result=$((result + exit_code))
echo "'RE=TestKube make test-run' exit status: $exit_code"

# Running long tests
cleanup
num_targets=6
num_proxies=6
{ echo $num_targets; echo $num_proxies; echo 4; echo 1; } | MODE="debug" make deploy
post_deploy $((num_proxies + num_targets))
make test-long && make test-aisloader
exit_code=$?
result=$((result + exit_code))
echo "'make test-long && make test-aisloader' exit status: $exit_code"

cleanup
if [[ $result -ne 0 ]]; then
  echo "tests failed"
fi

exit $result
