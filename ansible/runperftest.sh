#!/bin/bash
set -o xtrace
set -e

FILE='inventory/new_targets.txt'
WORKLOAD=$4
echo Third argument: "$WORKLOAD"
echo Remaining arguments: "$@"

cmd="./startclient.sh "$@
echo Running client cmd - $cmd
parallel-ssh -t 0 -h inventory/clients.txt -i $cmd

clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l`
sleep 20
count=0
while [ $clients_running -gt 0 ]; do 
	clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l` 
	echo running 
	parallel-ssh -h inventory/clients.txt -i 'tail -10 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/bench/dfcloader/screenlog.0'
	parallel-ssh -h inventory/targets.txt -i "iostat -xm 5 -c 2 | tail -33" || true
	parallel-ssh -h inventory/targets.txt -i "netstat -s | grep transmit" || true
	sleep 30
	count=$((count+1))
	if [ "$WORKLOAD" == "0" ]; then
		echo GET workload detected
		echo Count = $count
		if [ $count -eq 60 ]; then
			new_target=`head -n 1 $FILE`
			if [ ! -z "$new_target" ]; then
				tail -n +2 "$FILE" > "$FILE.tmp" && mv "$FILE.tmp" "$FILE" || true
				echo Adding target $new_target
				echo $new_target >> 'inventory/targets.txt'
				ssh $new_target 'cat dfc.json'
				ssh $new_target 'nohup /home/ubuntu/starttarget.sh >/dev/null 2>&1'
				count=0
			fi
		fi
	fi
done

parallel-ssh -h inventory/clients.txt -i 'tail -20 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/bench/dfcloader/screenlog.0'
