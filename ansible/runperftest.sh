#!/bin/bash
set -o xtrace
set -e

pctput=0
duration=120m
minsize=8192
maxsize=8192

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
        echo "Using default minzise -" $minsize
else
        minsize=$3
fi
if [ -z "$4" ]; then
        echo "Using default maxsize -" $maxsize
else
        maxsize=$4
fi
cmd="./startclient.sh $pctput $duration $minsize $maxsize"
parallel-ssh -t 0 -h inventory/clients.txt -i $cmd

clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l`
sleep 20
while [ $clients_running -gt 0 ]; do 
	clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l` 
	echo running 
	parallel-ssh -h inventory/clients.txt -i 'tail -10 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/cmd/dfcloader/screenlog.0'
	parallel-ssh -h inventory/targets.txt -i "iostat -xm 5 -c 2 | tail -33"
	parallel-ssh -h inventory/targets.txt -i "netstat -s | grep transmit"
	parallel-ssh -h inventory/targets.txt -i "tail -5 /var/log/dfc/dfc.ERROR"
	parallel-ssh -h inventory/proxy.txt -i "tail -5 /var/log/dfcproxy/dfc.ERROR"
	sleep 20 
done

parallel-ssh -h inventory/clients.txt -i 'tail -20 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/cmd/dfcloader/screenlog.0'
