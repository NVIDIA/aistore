#!/bin/bash -p

#
# Tar up logs of interest from an ais/k8s installation. Must be run from a host
# on which kubectl is configured to operate.
#

TOP=""

function whinge {
    echo "$*" >/dev/stderr
    [[ -n "$TOP" ]] && rm -rf $TOP
    exit 1
}

#
# Copy file(s) from pods matching the given selector, using kubectl cp. This information
# comes from the *current* pod container instances, obviously.
#
function copy_files {
    tag=$1
    selector=$2
    shift; shift

    tagdir=$TOP/$tag
    mkdir $tagdir || whinge "Failed to make $tagdir subdir"

    echo "Retrieving $@ ..."

    for pod in $(kubectl get pods -n $NAMESPACE --selector="$selector" -o=custom-columns=NAME:.metadata.name | tail -n +2)
    do
        destdir=$tagdir/$pod
        mkdir $destdir || whinge "Failed to make log subdir for pod $pod for tag $tag"
        kubectl exec -n $NAMESPACE $pod -- tar cf - "$@" | tar --directory=$destdir -x -f -
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

    echo "Retrieving container logs ..."

    for pod in $(kubectl get pods -n $NAMESPACE --selector="$selector" -o=custom-columns=NAME:.metadata.name | tail -n +2)
    do
        destdir=$tagdir/$pod
        mkdir $destdir || whinge "Failed to make log subdir for pod $pod for tag $tag"
        kubectl logs -n $NAMESPACE $pod > $destdir/kubectl-log.txt
        kubectl log -n $NAMESPACE --previous=true $pod > $destdir/kubectl-log-previous.txt 2>/dev/null
        [[ -s "$destdir/kubectl-log-previous.txt" ]] || rm -f $destdir/kubectl-log-previous.txt
    done
}

#
# Logs will be accumulated in a subdir of BASEDIR; BASEDIR must already exist.
# We'll make a subdirectory of BASEDIR names aislogs[-{group}]-YYYYMMDDhhmm-XXX
#

# Default basedir is TMPDIR or /tmp
BASEDIR=${TMPDIR:-/tmp}

# Default group name
GROUP=""

# Datestamp - likely enough that we don't need the XXX component of mktemp?
DATESTAMP=$(date +'%Y%m%d%H%M')

NAMESPACE=default

TEMP=$(getopt -o 'd:g:n:' -n 'gather_logs.sh' -- "$@")
if [ $? -ne 0 ]; then
        echo 'Terminating...' >&2
        exit 1
fi

eval set -- "$TEMP"
unset TEMP

while true; do
        case "$1" in
            '-d')
                BASEDIR="$2"
                shift; shift
                continue
            ;;

            '-g')
                GROUP="-$2"
                shift; shift
                continue
            ;;

            '-n')
                NAMESPACE="$2"
                shift; shift
            ;;

            '--')
                shift
                break
            ;;

            *)
                echo "Option parsing internal error" >&2
                exit 1
            ;;
        esac
done

[[ -d "$BASEDIR" ]] || whinge "BASEDIR $BASEDIR does not exist"
TEMPLATE="aislogs${GROUP}-$DATESTAMP-XXX"
TOP=$(mktemp -d --tmpdir=$BASEDIR -t "$TEMPLATE")
[[ -n "$TOP" ]] || whinge "Failed to create temporary directory for output"
echo "Gathering data into $TOP ..."

copy_files aispods 'app=ais' var/log/ais/ etc/ais var/log/aismisc
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

tardest=$BASEDIR/$(basename $TOP).tgz
(cd $TOP && tar cf - .) | gzip > $tardest

echo "Results browsable at $TOP, tar at $tardest"

exit 0