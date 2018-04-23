#!/bin/bash
set -o xtrace
set -e

cmd="./startclient.sh "$@
echo Running client cmd - $cmd
parallel-ssh -t 0 -h inventory/clients.txt -i $cmd

clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l`
sleep 20
while [ $clients_running -gt 0 ]; do 
	clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l` 
	echo running 
	parallel-ssh -h inventory/clients.txt -i 'tail -10 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/cmd/dfcloader/screenlog.0'
	parallel-ssh -h inventory/targets.txt -i "iostat -xm 5 -c 2 | tail -33" || true
	parallel-ssh -h inventory/targets.txt -i "netstat -s | grep transmit" || true
	sleep 30 
done

parallel-ssh -h inventory/clients.txt -i 'tail -20 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/cmd/dfcloader/screenlog.0'
