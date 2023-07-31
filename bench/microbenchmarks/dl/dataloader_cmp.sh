#!/bin/bash -p

#
# Control script to run a number of dataloader tests as specified in the
# given config file,  using a hardware topology (number of DGX nodes,
# GPUs per node) as specified in the config file.
#

function whinge {
	echo "FAILURE $@"
	exit 2
}

function usage {
  PROG=$1

  cat <<-EOU
    Usage: $PROG -c <testrc> -d <output-base-dir> -r <redis-svc> [-t <tag>] [-n] [-f] [-i <image>] [-s <pod-script>]
EOU
}

declare -i dryrun=0   # dry-run; 1 = create pods but don't train; 2 = only show intent
outdir=""
tag=""
redis_svc="bm-redis-master"
image="quay.io/nvidia/pytorch:hack11"	    # image for containers
flush=false
script='./scripts/run_dl_test.sh'

while getopts ":c:d:fhi:nr:s:t:" opt; do
  case $opt in
    c)  testrc=$OPTARG
        [[ -r $testrc ]] || whinge "proposed test config file $testrc is absent/unreadable" 
        ;;
    d)  outdir=$OPTARG
        [[ -d $outdir ]] || whinge "proposed output directory $outdir does not exist"
        ;;
    f)  flush=true
        ;;
    h)
        usage $0
        exit 1
        ;;
    i)  image=$OPTARG
        ;;
    n)  dryrun=$((dryrun + 1))
        ;;
    r)  redis=$OPTARG
        ;;
    s)  script=$OPTARG
        [[ -f $script ]] || whinge "$script is awol!"
        ;;
    t)
        tag="$t"
        ;;
    \?) usage $0
        exit 1
  esac
done

[[ -n "$testrc" ]] || whinge "specify a test config file using -c"
[[ -n "$outdir" ]] || whinge "specify an output parent directory using -d"

DATE="$(date +'%Y%m%d_%H%M')"
dest=$outdir/$DATE
logs=$dest/logs
mkdir -p $logs || whinge "failed to make output and log subdirectories"
echo "Run output will accumulate in $dest"

#
# Copy test config into output directory with a standard name, and add any test tag/comment.
#
cp $testrc $dest/testrc
[[ -n "$tag" ]] && echo "$tag" > $dest/tag

#
# Test infrastructure bits - no likely need to change these
#
masterhost=dmaster	  # distributed master, will be published in DNS for master pod
masterport=29500      # port to rendevouz on at master
groupname="dlbench"   # used to label headless service, and for group ops (delete) on pods we start
groupnum="$$"         # for multiple users of script (it's a start! clash on service name still)

#
# Token check that we're alone
#
[[ -z $(kubectl get pods --no-headers --selector=group=${groupname}) ]] || whinge "we are not alone"

#
# Grok redis master IP from the given service name. We require a (passwordless!)
# redis service to coordinate between this script and pods.
#
redis_ip=$(kubectl get service/${redis_svc} -o=jsonpath="{.spec.clusterIP}" 2>/dev/null)
[[ -n "$redis_ip " ]] || whinge "Could not lookup cluster-IP of redis service $redis_svc"

# this assumes we're running the control script on a
# cluster node
function rsignal {
  redis-cli -h ${redis_ip} "$@"
}

#
# Testcase globals
#
declare -i tc_numnodes
declare -i tc_ngpupernode
declare tc_mode
declare tc_backend
declare tc_ctx
declare tc_datapath
declare tc_batchsize
declare tc_cast
declare -i tc_epochs
declare -i tc_iters
declare tc_extra
declare tc_testloaders

#
# Create a configmap from the pod test script; specify key as 'runbench'
# regardless of script basename.
#
kubectl delete configmap bench-script >/dev/null 2>&1
kubectl create configmap bench-script --from-file=runbench=${script} || \
  whinge "failed to create bench script configmap"

#
# Create a pod to run a number of ranks of the distributed job, necessarily
# all on the same physical node (being in a single pod).
#
# The "number of nodes" in torch.distributed is realized by that number of
# pods. Depending on the gpu resource requests, it it possible that more than
# one pod could be scheduled on the same physical node. If fillorder is
# "widthfirst" then we use pod anti-affinity to spread pods over DGX
# systems before filling a system; "depthfirst" we try to influence scheduling
# to pack the pods in more densely.
#
function create_pod {
	local podnum=$1
  local fillorder=$2
  local reqnode=$3

	typeset master=false
	ports=""
	if [[ $podnum -eq 0 ]]; then
		master=true
		ports="\
    - name: distmaster
      containerPort: ${masterport}
      protocol: TCP"
	fi

  # nodeAffinity and node labeling may be nicer, but this'll do
  reqnodename=""
  if [[ $reqnode != "-" ]]; then
  reqnodename="\
  nodeName: "$reqnode"
    "
  fi

  if [[ $fillorder == "widthfirst" ]]; then
    aff_which="podAntiAffinity"
  elif [[ $fillorder == "depthfirst" ]]; then
    aff_which="podAffinity"
  else
    whinge "Unknown fill order $fillorder"
  fi

  affinity="\
  affinity:
    ${aff_which}:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: group
              operator: In
              values:
              - $groupname
          topologyKey: "kubernetes.io/hostname"
    "

  #
  # For the master pod create a headless clusterIP service so that other
  # pods can use the master pod DNS name to redevouz on.
  #
	if $master; then
	  kubectl apply -f - <<EOSVC
---
apiVersion: v1
kind: Service
metadata:
  name: "${masterhost}"
  labels:
    group: "${groupname}"
    groupnum: "${groupnum}"
spec:
  type: ClusterIP
  clusterIP: None
  selector:
    group: "${groupname}"
    groupnum: "${groupnum}"
    master: "true"
EOSVC

    [[ $? -ne 0 ]] && whinge "Failed to create headless service"
	fi

  #
  # Create pod; use a temp file so we can view any failing yaml we build
  #
  tf=$(mktemp)
	cat > $tf <<EOPOD
---
apiVersion: v1
kind: Pod
metadata:
  generateName: ${groupname}-${podnum}-
  labels:
    group: "${groupname}"
    groupnum: "${groupnum}"
    master: "${master}"
spec:
  containers:
  - name: dl
    image: ${image}
    resources:
      requests:
      limits:
        nvidia.com/gpu: "${tc_ngpupernode}"
    command: [ "/bin/bash" ]
    args: [ "/bench/runbench" ]
    env:
    - name: ARG_MASTER_HOST
      value: "${masterhost}"
    - name: ARG_MASTER_PORT
      value: "${masterport}"
    - name: ARG_NNODES
      value: "${tc_numnodes}"
    - name: ARG_NODE_RANK
      value: "${podnum}"
    - name: ARG_NPROC_PER_NODE
      value: "${tc_ngpupernode}"
    ports:
${ports}
    volumeMounts:
    - name: scripts
      mountPath: /scripts
    - name: bench-script
      mountPath: /bench
    - name: benchrc
      mountPath: /benchrc
    - name: dshm
      mountPath: /dev/shm
    - name: imagenet-nfs
      mountPath: /nfs/imagenet                      # 'val' here is a copy of 'train'
      readOnly: true
    - name: imagenet-inflated-train-nfs
      mountPath: /nfs/imagenet-inflated/train       # 'train' here is inflated; no inflated val, so ...
      readOnly: true
    - name: imagenet-inflated-val-nfs
      mountPath: /nfs/imagenet-inflated/val         # ... this also mounts the train set on val
      readOnly: true
    - name: imagenet-train-ssd                      # std imagenet on SSD of every DGX system
      mountPath: /data/imagenet/train
    - name: imagenet-val-ssd
      mountPath: /data/imagenet/val                 # we point this to train data below
    - name: imagenet-inflated-train-ssd             # only present on dgx18
      mountPath: /data/imagenet-inflated/train
    - name: imagenet-inflated-val-ssd               # only present on dgx18, point to train data
      mountPath: /data/imagenet-inflated/val
${reqnodename}
${affinity}
  volumes:
  - name: scripts
    configMap:
      name: scripts
  - name: bench-script
    configMap:
      name: bench-script
  - name: benchrc
    configMap:
      name: benchrc-cm
  - name: dshm
    emptyDir:
      medium: Memory
  - name: imagenet-nfs
    persistentVolumeClaim:
      claimName: imagenet-nfs-pvc
      readOnly: true
  - name: imagenet-inflated-train-nfs
    persistentVolumeClaim:
      claimName: imagenet-inflated-train-nfs-pvc
      readOnly: true
  - name: imagenet-inflated-val-nfs
    persistentVolumeClaim:
      claimName: imagenet-inflated-val-nfs-pvc
      readOnly: true
  - name: imagenet-train-ssd
    hostPath:
      path: /imagenet/train
  - name: imagenet-val-ssd
    hostPath:
      path: /imagenet/train           # not a typo
  - name: imagenet-inflated-train-ssd # only populated on dgx18, empty elsewhere
    hostPath:
      path: /imagenet/train-inflated
  - name: imagenet-inflated-val-ssd   # only populated on dgx18, empty elsewhere
    hostPath:
      path: /imagenet/train-inflated
  restartPolicy: Never
EOPOD

  kubectl create -f $tf
  if [[ $? -eq 0 ]]; then
    rm $tf
  else
    whinge "Failed to create pod; see yaml at $tf"
  fi
}

#
# Used to flush only components involved in a test; now uses
# ye olde blunderbuss.
#
# XXX Hardcoded bits here
#
function drop_caches {
  echo "Dropping caches in k8s cluster"
  ansible -f 24 -i $HOME/hosts.ini k8s-cluster -m shell -a 'echo 3 > /proc/sys/vm/drop_caches' --become >/dev/null

  echo "Dropping cache on single node AIS system"
  ansible -i $HOME/hosts.ini single -m shell -a 'echo 3 > /proc/sys/vm/drop_caches' --become >/dev/null

  echo "Dropping cache on single node NFS server"
  ansible -i $HOME/hosts.ini nfs -m shell -a 'echo 3 > /proc/sys/vm/drop_caches' --become >/dev/null
}

function testit {
  local pfx=$1
  local fillorder=$2

  #
  # For webdataset, form the full datapath
  #
  if [[ $tc_backend =~ webdataset ]]; then
    tc_datapath=${tc_ctx}/v1/objects/$tc_datapath
  fi

  local summary="$tc_numnodes node(s) $tc_mode with $tc_ngpupernode GPU per node; "
  summary+="$tc_backend over $tc_datapath ($tc_extra) bs $tc_batchsize cast $tc_cast "
  summary+="${tc_epochs}x${tc_iters}"

  reqnode="-"
  # special case - the inflated imagenet dataset for SSD is only available on dgx18
  if [[ $tc_datapath == "/data/imagenet-inflated" ]]; then
    [[ $tc_numnodes -eq 1 ]] || whinge "The inflated imagenet set for SSD is only populated on one node!"
    reqnode="dgx18"
  fi

  echo "***** $summary"
  [[ $dryrun -ge 2 ]] && return

  if [[ $dryrun -ne 0 ]]; then
    pod_dryrun=true
  else
    pod_dryrun=false
    $flush && drop_caches
  fi

  #
  # Create a config map to configure the run in the pod
  #
  tf=$(mktemp) || whinge "failed to create temporary file"
  cat > $tf <<-EOCM
    #
    # sourced by script in pod to control training/inference
    #

    what=$tc_mode
    be=$tc_backend
    ds=$tc_datapath
    bs=$tc_batchsize
    cast=$tc_cast
    epochs=$tc_epochs
    prof=$tc_iters
    testloaders=$tc_testloaders
    redis_svc=${redis_svc}
    dryrun=$pod_dryrun

    #
    # for webdataset backends
    #
    export openbufsize=32768
    export tarbufsize=524288
    export shards="${tc_extra}"
    export val="${tc_extra}"
EOCM

  kubectl delete cm/benchrc-cm 2>/dev/null
  kubectl create cm benchrc-cm --from-file=benchrc=$tf || whinge "failed to create configmap"
  rm $tf

  # Some very crude synchronization between control script and pods ...
  rsignal DEL benchr
  rsignal DEL benchc

		for ((i=0; i<tc_numnodes; i=i+1)); do
			create_pod $i $fillorder $reqnode
		done

  #
  # Check and report pods confirmed as running the test
  # XXX No allowance for pod failures!
  #
  sleep 5
	while true; do
			running=$(rsignal GET benchr)
			completed=$(rsignal GET benchc)

			echo "${running:-0} pods of $tc_numnodes confirmed running, ${completed:-0} completed ($summary)"
			if [[ -n "$completed" ]]; then
				[[ $completed -eq $tc_numnodes ]] && break
			fi

			sleep 10
		done

    # prefix is passed with testnum to avoid output collisions
    if [[ $dryrun -eq 0 ]]; then
 		  rf="${pfx}-${tc_mode}-${tc_backend}-${tc_ctx}-${tc_batchsize}-${tc_cast}"
		  masterpod=$(kubectl get pods --no-headers --selector=group=${groupname},groupnum=${groupnum},master=true | awk '{print $1}'   )
		  echo "All pods done, grabbing results from pod $masterpod to $dest/${rf}.json"
		  kubectl cp ${masterpod}:/results/result.json $dest/${rf}.json
    fi

		echo "Preserving pod logs ..."
		for p in $(kubectl get pods --no-headers --selector=group=${groupname},groupnum=${groupnum} | awk '{print $1}'); do
			kubectl logs $p > $logs/${rf}-$p-log.out
		done 

		echo "Deleting pods ..."
		kubectl delete pods --selector=group=${groupname},groupnum=${groupnum}
		sleep 5
}

#
# Source testrc file
#
. $testrc || whinge "error in sourcing $testrc"

for testset in ${enabled_sets[@]}; do
  declare config_worlds=""
  declare config_fillorder="widthfirst"

  for pass in planning doing; do
    declare -i testnum=1
    tc_testloaders=false

    eval echo "\"\$${testset}\"" | while read line; do
      if [[ $line =~ = ]]; then
          [[ $testnum -eq 1 ]] || whinge "set $testset - cannot change test config once test spec start"
          [[ $line =~ config_.*= ]] || whinge "set $testset illegal variable in test $testnum"
          eval $line
        continue
      fi

      # check required vars are set
      [[ -n "$config_worlds" ]] || whinge "set $testset missing config_worlds assignment"

      set -- $line
      declare modes=$1
      tc_backend=$2
      tc_ctx=$3
      tc_datapath=$4
      tc_batchsize=$5
      tc_cast=$6
      tc_epochs=$7
      tc_iters=$8
      tc_extra=$9
      [[ -n "$config_testloaders" ]] && tc_testloaders=$config_testloaders

      if [[ $modes == "both" ]]; then
        modes="training inference"
      elif [[ $modes == "-" ]]; then
        continue
      elif [[ $modes != "training" && $modes != "inference" ]]; then
        whinge "Unexpected mode $modes in $testset test $testnum"
      fi

      if [[ $mode == "planning" ]]; then
        testnum=$((testnum + 1))
        continue
      fi

      #
      # expand context (gateway url for AIS)
      #
      if [[ "${tc_ctx}" != "-" ]]; then
        tc_ctx=$(eval echo \$config_${tc_ctx})
      fi

      #
      # expand extra (shard pattern for AIS)
      #
      if [[ "${tc_extra}" != "-" ]]; then
        tc_extra=$(eval echo \$config_${tc_extra})
      fi

      for topo in $(echo $config_worlds | tr ',' ' '); do
        tc_numnodes=$(echo $topo | cut -d x -f 1)
        tc_ngpupernode=$(echo $topo | cut -d x -f 2)
        for tc_mode in $modes; do
          testit "${testset}-${testnum}" $config_fillorder
        done
      done

      testnum=$((testnum + 1))

    done
  done
done
