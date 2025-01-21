if [[ "$#" -lt 1 ]]; then
    # Print all mountpaths.
    echo "available mountpaths (pass as args to run):"
    lsblk | grep -e "disk" -e "raid" -e "part" | awk '{print $7}' | grep -ve "^$"
    exit 0
fi

for mountpath in "$@"; do
    lsblk | grep -e "disk" -e "raid" -e "part" | awk '{print $7}' | grep -ve "^$" | grep -Fx "${mountpath}" &> /dev/null
    if [[ "$?" -ne 0 ]]; then
      echo "${mountpath} is not in a valid mountpath (re-run without args to get list)"
      exit 1
    fi
done

if [[ -z "$seconds" ]]; then
    seconds=50
fi
if [[ "$seconds" =~ ^[\-0-9]+$ ]] && (( "$seconds" > 0)); then
  echo "Benchmark running for $seconds seconds."
else
  echo "'${seconds}' is not a positive integer"
  exit 1
fi

if [[ -z "$workers" ]]; then
    workers=1
fi
if [[ "$workers" =~ ^[\-0-9]+$ ]] && (( "$workers" > 0)); then
  echo "Running $workers workers."
else
  echo "'${workers}' is not a positive integer"
  exit 1
fi

if [[ -z "$iobatch" ]]; then
    iobatch=1000
fi
if [[ "$iobatch" =~ ^[0-9]*[1-9][0-9]*$ ]]; then
  echo "Performing $iobatch operations per batch"
else
  echo "$iobatch is not a number greater than 1"
  exit 1
fi


if [[ -z "$pct_read" ]]; then
    pct_read=75
fi
if [[ "$pct_read" =~ ^[\-0-9]+$ ]] && (( "$pct_read" >= 0)) && (( "$pct_read" <= 100)); then
  echo "Doing $pct_read percent read."
else
  echo "'${pct_read}' is not a valid percentage"
  exit 1
fi

type -P "fio" > /dev/null
if [[ "$?" -ne 0 ]]; then
  echo "fio not installed"
  exit 1
fi

if [[ -z "$(ldconfig -p | grep libaio)" ]]; then
  echo "libaio not installed"
  exit 1
fi

foldername="dutil-bench-stress-dir"
files=""
i=0
for mountpath in "$@"; do
    if [[ ! -z $files ]]; then
       files="${files}:"
    fi

    mpath="$(echo -n ${mountpath} | sed -e "s,/\+$,,")/${foldername}"
    rm -rf ${mpath}
    mkdir -m 777 -p ${mpath}

    i=$((i + 1))
    file="${mpath}/$i"
    files="${files}${file}"
done

confname=/tmp/dutil-bench-stress.io
rm -rf $confname
cat >$confname <<EOL
[global]
bs=8k
iodepth=128
direct=1
ioengine=libaio
randrepeat=0
group_reporting
time_based
runtime=${seconds}
filesize=2G
thread

[job1]
rw=randrw
rwmixread=${pct_read}
numjobs=${workers}
filename=${files}
file_service_type=random:${iobatch}
name=random-bench
EOL

echo "start time: $(date +\"%D.%T.%N\")"
fio $confname
echo "end time: $(date +\"%D.%T.%N\")"

rm -rf $confname

for mountpath in "$@"; do
    mpath="$(echo -n ${mountpath} | sed -e "s,/\+$,,")/${foldername}"
    rm -rf ${mpath}
done
