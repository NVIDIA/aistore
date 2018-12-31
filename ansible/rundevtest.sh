#!/bin/bash
set -o xtrace
source /etc/profile.d/dfcpaths.sh
source aws.env
for dfcpid in `ps -C dfc -o pid=`; do echo Stopping DFC $dfcpid; sudo kill $dfcpid; done
sudo rm -rf /home/ubuntu/.dfc*
rm -rf /tmp/dfc*
cd $DFCSRC
if [ ! -z $1 ]; then
    echo Git checkout branch $1
    git checkout $1
fi

git fetch --all
git reset --hard origin/master
git status
git log | head -5

setup/deploy.sh -loglevel=3 -statstime=10s <<< $'4\n3\n2\n1'

echo sleep 10 seconds before checking DFC process
sleep 10
dfcprocs=$(ps -C dfc -o pid= | wc -l)
echo number of dfcprocs $dfcprocs
if [ $dfcprocs -lt 7 ]; then
    echo dfc did not start properly
    exit 1
fi

echo Working with DFC build
grep -r Build /tmp/dfc | head -1

cd ../
cdir=$(pwd)
echo Run go tests from $cdir

echo run DFC tests with cloud bucket devtestcloud
BUCKET=devtestcloud go test -v -p 1 -count 1 -timeout 60m ./...

cloudExitStatus=$?
echo devtest exit status $cloudExitStatus

for dfcpid in `ps -C dfc -o pid=`; do echo Stopping DFC $dfcpid; sudo kill $dfcpid; done
result=0
if [ $cloudExitStatus -ne 0 ]; then
    echo DevTests failed
    result=$((result + 1))
fi

exit $result
