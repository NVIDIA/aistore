#!/bin/bash

set -e

# Default parameters
bucket_name=""
duration="1m"
num_workers=8
quiet_flag="--quiet"

# Parse arguments
for i in "$@"; do
case $i in
    -b=*|--bucket=*|bucket=*)
        bucket_name="${i#*=}"
        shift
        ;;
    -d=*|--duration=*|duration=*)
        duration="${i#*=}"
        shift
        ;;
    -w=*|--workers=*|workers=*)
        num_workers="${i#*=}"
        shift
        ;;
    -v|--verbose)
        quiet_flag=""
        shift
        ;;
    *)
        echo "Usage: $0 [--bucket=NAME] [--duration=TIME] [--workers=N] [--verbose]"
        exit 1
        ;;
esac
done

# Generate random bucket name if not provided
if [[ -z ${bucket_name} ]]; then
  bucket_name="$(tr -dc 'a-z0-9' </dev/urandom | head -c 12)"
fi

# Parse duration to seconds for calculations
parse_duration() {
    local d=$1
    if [[ $d =~ ^([0-9]+)m$ ]]; then
        echo $((${BASH_REMATCH[1]} * 60))
    elif [[ $d =~ ^([0-9]+)s$ ]]; then
        echo ${BASH_REMATCH[1]}
    else
        echo 60  # default to 60s if unparseable
    fi
}

# Calculate stage durations
duration_sec=$(parse_duration "$duration")

# Stage 1 & 2: use full duration
stage1_dur="${duration}"
stage2_dur="${duration}"

# Stages 3-5: use half duration, but clamp to 30-120s range
stage3_sec=$((duration_sec / 2))
stage3_sec=$((stage3_sec < 30 ? 30 : stage3_sec))
stage3_sec=$((stage3_sec > 120 ? 120 : stage3_sec))
stage3_dur="${stage3_sec}s"
stage4_dur="${stage3_sec}s"
stage5_dur="${stage3_sec}s"

# Calculate total time (with ~10s overhead per stage)
total_sec=$((duration_sec * 2 + stage3_sec * 3 + 50))
total_min=$((total_sec / 60))
total_rem=$((total_sec % 60))

echo ""
echo "================================================================================"
echo "                             aisloader CI Test"
echo "================================================================================"
echo "  Bucket:       ais://${bucket_name}"
echo "  Workers:      ${num_workers}"
echo "  Est. Runtime: ~${total_min}m${total_rem}s"
echo "================================================================================"
echo ""

# Stage 1
echo "=== Stage 1/5: Populate (100% PUT, 10GiB, 1-10MiB) [${stage1_dur}] ============"
aisloader -bucket="${bucket_name}" \
    -duration="${stage1_dur}" \
    -pctput=100 \
    -provider=ais \
    -maxsize=10MiB \
    -minsize=1MiB \
    -totalputsize=10GiB \
    -cleanup=false \
    -numworkers="${num_workers}" \
    ${quiet_flag}

echo ""
echo "=== Stage 2/5: Mixed PUT/MPU (100% PUT, 30% chunked, 5GiB) [${stage2_dur}] ======"
aisloader -bucket="${bucket_name}" \
    -duration="${stage2_dur}" \
    -pctput=100 \
    -provider=ais \
    -maxsize=20MiB \
    -minsize=5MiB \
    -totalputsize=5GiB \
    -cleanup=false \
    -numworkers="${num_workers}" \
    -multipart-chunks=4 \
    -pctmultipart=30 \
    ${quiet_flag}

echo ""
echo "=== Stage 3/5: Mixed PUT/GET (10% PUT, 90% GET) [${stage3_dur}] =================="
aisloader -bucket="${bucket_name}" \
    -duration="${stage3_dur}" \
    -pctput=10 \
    -provider=ais \
    -maxsize=10MiB \
    -minsize=1MiB \
    -cleanup=false \
    -numworkers="${num_workers}" \
    ${quiet_flag}

echo ""
echo "=== Stage 4/5: GetBatch (num=32) [${stage4_dur}] ================================"
aisloader -bucket="${bucket_name}" \
    -duration="${stage4_dur}" \
    -pctput=0 \
    -provider=ais \
    -cleanup=false \
    -numworkers=4 \
    -get-batchsize=32 \
    ${quiet_flag}

echo ""
echo "=== Stage 5/5: GetBatch (num=64) + cleanup [${stage5_dur}] ====================="
aisloader -bucket="${bucket_name}" \
    -duration="${stage5_dur}" \
    -pctput=0 \
    -provider=ais \
    -cleanup=true \
    -numworkers=4 \
    -get-batchsize=64 \
    ${quiet_flag}

cat <<'footer'

================================================================================
                      All tests completed successfully
================================================================================

footer
