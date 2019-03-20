#!/bin/bash -p

#
# Tar up logs of interest from an ais/k8s installation. Must be run from a host
# on which kubectl is configured to operate.
#

function whinge {
    echo "$*" >/dev/stderr
    [[ -n "$TOP" ]] && rm -rf $TOP
    exit 1
}

TOP=$(mktemp -t -d aislogs.XXXXXX)
[[ -n "$TOP" ]] || whinge "Failed to create temporary directory for output"
echo "Gathering data into $TOP ..."

#
# Copy file(s) from pods matching the given selector, using kubectl cp. This information
# comes from the *current* pod container instances, obviously.
#
function copy_files {
    tag=$1
    selector=$2
    pfx=$3

    tagdir=$TOP/$tag
    mkdir $tagdir || whinge "Failed to make $tagdir subdir"

    echo "Retrieving all $pfx ..."

    for pod in $(kubectl get pods --selector="$selector" -o=custom-columns=NAME:.metadata.name | tail -n +2)
    do
        destdir=$tagdir/$pod
        mkdir $destdir || whinge "Failed to make log subdir for pod $pod for tag $tag"
        kubectl cp $pod:$pfx $destdir
    done
}

#
# Retrieve kubectl logs from the current and previous (if any) pod incarnations.
#
function get_k8s_pod_logs {
    tag=$1
    selector=$2

    tagdir=$TOP/$tag
    mkdir $tagdir || whinge "Failed to make $tagdir subdir"

    echo "Retrieving kubectl logs ..."

    for pod in $(kubectl get pods --selector="$selector" -o=custom-columns=NAME:.metadata.name | tail -n +2)
    do
        destdir=$tagdir/$pod
        mkdir $destdir || whinge "Failed to make log subdir for pod $pod for tag $tag"
        kubectl logs $pod > $destdir/kubectl-log.txt
        kubectl log --previous=true $pod > $destdir/kubectl-log-previous.txt 2>/dev/null
        [[ -s "$destdir/kubectl-log-previous.txt" ]] || rm -f $destdir/kubectl-log-previous.txt
    done
}

copy_files aislogs 'app=ais' var/log/ais/
copy_files aisstate 'app=ais' etc/ais/
copy_files aismisc 'app=ais' var/log/aismisc
copy_files aisloader 'app.kubernetes.io/name=aisloader-stress' var/log/aisloader 

get_k8s_pod_logs kubectl_logs 'app=ais'

#
# Ad-hoc files additions from the cmdline
#
while [[ $# -gt 0 ]]; do
    pfxarg=$1
    shift
    usertag=$(echo $pfxarg | tr '/' '_')
    copy_files "$usertag" "$pfxarg"
done

tardest=/tmp/$(basename $TOP).tgz
(cd $TOP && tar cf - .) | gzip > $tardest

echo "Results browsable at $TOP, tar at $tardest"

exit 0