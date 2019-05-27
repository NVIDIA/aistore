#!/bin/bash
set -o xtrace
source /etc/profile.d/aispaths.sh
source aws.env
for pid in `ps -C aisnode -o pid=`; do echo Stopping AIStore $pid; sudo kill $pid; done
sudo rm -rf /home/ubuntu/.ais*
rm -rf /tmp/ais*
cd $AISSRC
if [ ! -z $1 ]; then
    echo Git checkout branch $1
    git checkout $1
fi

git fetch --all
git reset --hard origin/master
git status
git log | head -5

setup/deploy.sh -loglevel=3 -stats_time=10s <<< $'4\n4\n4\n1'

echo sleep 10 seconds before checking AIStore process
sleep 10
aisprocs=$(ps -C aisnode -o pid= | wc -l)
echo number of aisprocs $aisprocs
if [ $aisprocs -lt 8 ]; then
    echo aisnode did not start properly
    exit 1
fi

echo Working with AIStore build -
grep -r Build /tmp/ais | head -1

cd ../
cdir=$(pwd)
echo Run go tests from $cdir

echo run AIStore tests with cloud bucket devtestcloud

BUCKET=devtestcloud go test -v -p 1 -count 1 -timeout 120m ./...

cloudExitStatus=$?
echo devtest exit status $cloudExitStatus

for aispid in `ps -C aisnode -o pid=`; do echo Stopping AIStore $aispid; sudo kill $aispid; done
result=0
if [ $cloudExitStatus -ne 0 ]; then
    echo DevTests failed
    result=$((result + 1))
fi

exit $result
