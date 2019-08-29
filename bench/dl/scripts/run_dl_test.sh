#
# Run in container with the following in env:
#
# ARG_NNODES
# ARG_NODE_RANK
# ARG_MASTER_HOST
# ARG_MASTER_PORT
# ARG_NPROC_PER_NODE
#

#
# This is a clone with modified main_wl.py, and supporting dataloaders.py.
#
RN50="/root/DeepLearningExamples/PyTorch/Classification/RN50v1.5"

#
# WS to create and populate with results
#
WS=/results
rm -rf $WS	# committed image has past results :-(

function whinge {
	echo "FAILURE $@" >&2
	exit 2
}

mkdir -p $WS || whinge "Could not make results workspace $WS"
cd $RN50 || whinge "Could not chdir to $RN50"

function rsignal {
        redis-cli -h $redis_svc "$@"
}

try=0
while [[ $try -lt 30 ]]; do
	ping -c 1 $ARG_MASTER_HOST
	[[ $? -eq 0 ]] && break
	try=$((try + 1))
	sleep 1
done
[[ $try -eq 30 ]] && whinge "Could not ping $ARG_MASTER_HOST"

function runit {
	typeset backend=$1
	typeset data=$2
	typeset -i bs=$3
	typeset cast=$4
	typeset -i epochs=$5
	typeset -i prof=$6
	typeset tl=$7
	shift; shift; shift; shift; shift; shift; shift

	usecast="$cast"
	[[ "$cast" == "none" ]] && usecast=""
	[[ "$cast" == "fp32" ]] && usecast=""
	[[ $prof -eq 0 ]] && prof=""

	typeset raport="result.json"

	usetl=""
	$tl && usetl="--test-loaders"

	dist=""
	if [[ $((ARG_NNODES * ARG_NPROC_PER_NODE)) -gt 1 ]]; then
		dist="./multiproc.py \
		--nnodes $ARG_NNODES \
		--node_rank $ARG_NODE_RANK \
		--master_addr $ARG_MASTER_HOST \
		--master_port $ARG_MASTER_PORT \
		--nproc_per_node $ARG_NPROC_PER_NODE"
	fi

	set -x
	python3 $dist \
		./main_wl.py \
		--workspace $WS \
		--tag "${ARG_NNODES} nodes ${ARG_NPROC_PER_NODE} GPU per node" \
		--raport-file $raport \
		-j5 \
		-p 1 \
		-b $bs \
		${usecast:+--$usecast} \
		--epochs $epochs ${prof:+--prof $prof} ${usetl} \
		--data-backend $backend \
		"$@" \
		$data
	set +x
}

function train {
	typeset be=$1
	typeset ds=$2
	typeset -i bs=$3
	typeset cast=$4
	typeset -i epochs=$5
	typeset -i prof=$6
	typeset tl=$7

	typeset -i obs=$((bs * ARG_NPROC_PER_NODE * ARG_NNODES))

	runit $be $ds $bs $cast $epochs $prof $tl \
		--training-only --lr 2.048 \
		--optimizer-batch-size $obs \
		--warmup 8 --arch resnet50 -c fanin \
		--label-smoothing 0.1 --lr-schedule cosine --mom 0.875 --wd 3.0517578125e-05
}

function inference {
	typeset be=$1
	typeset ds=$2
	typeset -i bs=$3
	typeset cast=$4
	typeset -i epochs=$5
	typeset -i prof=$6
	typeset tl=$7

	runit $be $ds $bs $cast $epochs $prof $tl \
		--arch resnet50 --evaluate
}

# hack because no Dockerfile at the moment
# cat main_wl.py | sed 's/torch.set_num_threads(8)/torch.set_num_threads(4)/' > main_wl_fixed.py

# read config
[[ -f /benchrc/benchrc ]] || whinge "/bench/benchrc absent"
. /benchrc/benchrc || whinge "sourcing benchrc failed"

# benchrc supplies ${redis_svc}
rsignal INCR benchr

if [[ $what == "inference" ]]; then
	echo inference $be $ds $bs $cast $epochs $prof $testloaders
	$dryrun || inference $be $ds $bs $cast $epochs $prof $testloaders
else
	echo train $be $ds $bs $cast $epochs $prof $testloaders
	$dryrun || train $be $ds $bs $cast $epochs $prof $testloaders
fi

rsignal INCR benchc

sleep 1000	# control script cleans up
