#!/bin/bash
set -o xtrace
set -e

bucket=`hostname`
pctput=0
duration=120m

if [ -z "$1" ]; then
	echo "Using default pctput 0"
else
	pctput=$1
fi
if [ -z "$2" ]; then
	echo "Using default duration 120m"
else
	duration=$2
fi
if [ -z "$3" ]; then
	echo "Using default bucket -" $bucket
else
	bucket=$3
fi

source /etc/profile.d/dfcpaths.sh
cd $DFCSRC/../cmd/dfcloader

bucket=`hostname`
screen -mdSL client go run main.go worker.go -ip=10.0.1.170 -port=8081 -bucket=$bucket -local=true -minsize=8192 -maxsize=8192 -statsinterval=1 -readertype=rand -cleanup=false -pctput=$1 -duration=$2 -totalputsize=4048000000 -numworkers=64
