#!/bin/bash

set -o xtrace
source /etc/profile.d/aispaths.sh
source aws.env

for pid in $(ps -C aisnode -o pid=); do echo "stopping aisnode: $pid"; sudo kill $pid; done
cd $AISSRC && cd ..

if [[ -n $1 ]]; then
    echo "git checkout branch: $1"
    git checkout $1
fi

git fetch --all
git reset --hard origin/master
git status
git log | head -5

make clean
make aisloader
MODE="debug" make deploy <<< $'6\n6\n4\n1'

echo "sleep 10 seconds before checking AIStore processes"
sleep 10

nodes=$(ps -C aisnode -o pid= | wc -l)
echo "number of started aisprocs: $nodes"
if [[ $nodes -lt 8 ]]; then
    echo "some of the aisnodes did not start properly"
    exit 1
fi

echo "working with build: $(git rev-parse --short HEAD)"
echo "run tests with cloud bucket: nvais"

BUCKET=nvais make test-long && make test-aisloader
exit_code=$?

echo "'make test-long && make test-aisloader' exit status: $exit_code"

for pid in $(ps -C aisnode -o pid=); do echo "stopping aisnode: $pid"; sudo kill $pid; done
result=0
if [[ $exit_code -ne 0 ]]; then
    echo "tests failed"
    result=$((result + 1))
fi

exit $result
