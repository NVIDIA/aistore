#!/bin/bash
set -e

PROXYIP=`cat /home/ubuntu/inventory/proxy.txt`
PROXYPORT='8081'

bucket=`hostname`
pctput=0
duration=120m
minsize=8192
maxsize=8192
threads=64
nbuckets=1

# Parse args
while getopts ":b:p:d:m:x:t:n:" opt; do
    case $opt in
        b)
            echo "Using bucket suffix $OPTARG"
            bucket=`hostname`$OPTARG
            ;;
        p)
            echo "Using pctput $OPTARG"
            pctput=$OPTARG
            ;;
        d)
            echo "Using duration of $OPTARG"
            duration=$OPTARG
            ;;
        m)
            echo "Using minsize of $OPTARG"
            minsize=$OPTARG
            ;;
        x)
            echo "Using maxsize of $OPTARG"
            maxsize=$OPTARG
            ;;
        t)
            echo "Using dfcloader thread count of $OPTARG per bucket"
            threads=$OPTARG
            ;;
        n)
            echo "Using number of buckets $OPTARG"
            nbuckets=$OPTARG
            ;;

        \?)
            echo "Invalid option: -$OPTARG"
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument"
            exit 1
            ;;
    esac
done

source /etc/profile.d/dfcpaths.sh
cd $DFCSRC/../cmd/dfcloader
sudo rm -rf screenlog.0
screen -mdSL client go run main.go worker.go -ip=$PROXYIP -port=$PROXYPORT -bucket=$bucket -local=true -minsize=$minsize -maxsize=$maxsize -statsinterval=1 -readertype=rand -cleanup=false -pctput=$pctput -duration=$duration -totalputsize=4048000000 -numworkers=$threads

echo "started dfcloader, wait for screnlog file to show up with timeout of 2min"
x=0
while [ "$x" -lt 24 -a ! -f screenlog.0 ]
do
  sleep 5
  x=$((x+1))

done

echo "screenlog file created"
if grep -q 'Failed to boot strap' screenlog.0; then
	echo 'Failed to boot strap, restarting one more time'
	sudo rm -rf screenlog.0
	screen -mdSL client go run main.go worker.go -ip=$PROXYIP -port=$PROXYPORT -bucket=$bucket -local=true -minsize=$minsize -maxsize=$maxsize -statsinterval=1 -readertype=rand -cleanup=false -pctput=$pctput -duration=$duration -totalputsize=4048000000 -numworkers=64
	echo "started dfcloader, wait for screnlog file to show up with timeout of 2min"
	x=0
	while [ "$x" -lt 24 -a ! -f screenlog.0 ]
	do
	  sleep 5
	  x=$((x+1))

	done

	echo "screenlog file created"
fi