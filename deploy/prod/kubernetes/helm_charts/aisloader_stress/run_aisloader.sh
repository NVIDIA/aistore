#!/bin/bash -p 

function whinge {
	echo "$@" >&2
	exit 1
}

[[ $# -ne 2 ]]&& whinge "Add a tag and comment to cmdline"

TAG="$1"
WAFFLE="$2"

DATESTAMP=$(date +'%Y%m%d%H%M')

if [[ $(helm ls --all loader 2>/dev/null | wc -l) -ne 0 ]]; then
	echo "Helm release loader already present!" >&2
	exit 1
fi

BASEDIR=${BASEDIR:=$HOME/results/aisloader}
OUTDIR=$BASEDIR/aisloader-${TAG}-${DATESTAMP}

mkdir -p $OUTDIR || whinge "Failed to mkdir $OUTDIR"
echo "Output to $OUTDIR"

cp $0 $OUTDIR

kubectl get nodes --show-labels > $OUTDIR/kubectl-nodes.out 2>&1
kubectl get pods -o=wide > $OUTDIR/kubectl-pods.out 2>&1

instances=$(grep 'aisloader=yes' $OUTDIR/kubectl-nodes.out | wc -l)

WORKERS=${WORKERS:=250}
# Workers per aisloader client
#WORKERS=65
#WORKERS=500

# Bucket(s) - %s will expand to client hostname
BUCKETSPEC=${BUCKETSPEC:=sharded_huge_8M_bucket}
#BUCKETSPEC=huge_8M_bucket
#BUCKETSPEC=big_8M_bucket_%s
#BUCKETSPEC=sharded_big_8M_bucket_%s
#BUCKETSPEC=small_8M_bucket
#BUCKETSPEC=tiny_8M_bucket

# Min/max object read
MINOBJ=${MINOBJ:=8M}
MAXOBJ=${MAXOBJ:=8M}

# Run duration in minutes
DURATION=${DURATION:=15}
#DURATION=5
#DURATION=120

# Percent puts
PCTPUT=${PCTPUT:=0}
#PCTPUT=100

# Max numbers of puts (unlimited if 0)
MAXPUTS=${MAXPUTS:=0}

# Shard generated objects into this number of sub-prefixes (0 means do no shard)
PUTSHARDS=${PUTSHARDS:=1000}

# iostat period
IOSTAT_PERIOD=10

cat <<EOM | tee $OUTDIR/description
Run at: $DATESTAMP
Aisloader instances: $instances
Workers per instance: $WORKERS
Bucket: $BUCKETSPEC
Read sizes: $MINOBJ/$MAXOBJ
Duraton: $DURATION minutes
Puts: ${PCTPUT}%
$WAFFLE
EOM


# -------------

# Number of iostat periods in duration, plus a few
IOSTAT_REPEAT=$(( $DURATION * 60 / $IOSTAT_PERIOD + 6 ))

echo "Grab load averages ..."
ansible -f 50 -i $HOME/hosts.ini k8s-cluster -m shell -a 'uptime' > $OUTDIR/load-avg.out 2>&1



echo "Dropping caches .."
ansible -f 50 -i $HOME/hosts.ini cpu-worker-node -m shell -a 'sync; echo 3 > /proc/sys/vm/drop_caches' --become
sleep 5

OUTPUT=$OUTDIR/iostat-xc.all.out.$DATESTAMP
echo "Start iostat on all cpu nodes, to $OUTPUT ..."
ansible -f 50 -i $HOME/hosts.ini cpu-worker-node -m shell -a "iostat -xc $IOSTAT_PERIOD $IOSTAT_REPEAT" > $OUTPUT 2>&1 &
ANSPID=$!
sleep 5

# Note: --set-string on maxputs otherwise Helm renders as things like 1.7e+07 instead of 17000000

echo "Performing helm install"
helm install \
	--name loader \
	--set ais_release=demo \
	--set image.tag=23 \
	--set image.pullSecret=gmaltby-pull-secret \
	--set aisloaderArg.bucket.default=$BUCKETSPEC \
	--set aisloaderArg.pctput.default=$PCTPUT \
	--set-string aisloaderArg.maxputs.default=$MAXPUTS \
	--set aisloaderArg.putshards.default=$PUTSHARDS \
	--set aisloaderArg.duration.default=${DURATION}m \
	--set aisloaderArg.minsize.default=$MINOBJ \
	--set aisloaderArg.maxsize.default=$MAXOBJ \
	--set aisloaderArg.statsinterval.default=10 \
	--set aisloaderArg.uniquegets.default=true \
	--set aisloaderArg.numworkers.default=$WORKERS  charts/.

# would be nice to confirm completion, but the pods hang around awaiting termination

echo "Waiting ..."
wait $ANSPID

# get this from a better place some day

echo "Gathering logs"
/tmp/gather_logs.sh -d $OUTDIR -g $TAG

helm delete --purge loader
sleep 45
