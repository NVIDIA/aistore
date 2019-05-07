#!/bin/bash -p

# Clumsy script to repeat benchmark runs for differing configs

if [[ $# -ne 1 ]]; then
	echo "Need to know number of targets as only arg"
	exit 2
fi

TGTS=$1

export BASEDIR=$HOME/results/${TGTS}_targets

function dolabels {
	nodes=$1

	kubectl label nodes --all aisloader- >/dev/null 2>&1
	
	i=1
	while [[ $i -le $nodes ]]; do
		kubectl label node dgx$(printf "%02d" $i) aisloader=yes
		i=$((i + 1))
	done
}

while read line
do
	set -- $line

	runid="${TGTS}tgts_$1"
	echo "Run id $runid starting at $(date)..."

	dolabels $2

	export WORKERS=$3
	export BUCKETSPEC=$4
	export MINOBJ=$5; export MAXOBJ=$5
	export PCTPUT=$6
	export DURATION=$7

	kubectl get nodes --show-labels | grep aisloader=yes

	# this is the greedy one
	# sudo rm -rf /data/graphite/whisper/stats/timers

	./run_aisloader.sh $runid "$TGTS targets; $2 x $WORKERS; $BUCKETSPEC $MINOBJ/$MAXOBJ $PCTPUT puts for $DURATION minutes"
	echo "Snooze 5 minutes at $(date)"
	sleep 300
done <<-EOM
1	1	300	sharded_huge_8M_bucket	8M	0	60
2	2	300	sharded_huge_8M_bucket	8M	0	60
3	3	300	sharded_huge_8M_bucket	8M	0	60
4	4	300	sharded_huge_8M_bucket	8M	0	60
5	9	300	sharded_huge_8M_bucket	8M	0	60
EOM


