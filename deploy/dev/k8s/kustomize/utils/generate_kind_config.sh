#!/bin/bash

NUM_PROXY=${NUM_PROXY}
NUM_TARGET=${NUM_TARGET}

NUM_WORKERS=$(( NUM_PROXY > NUM_TARGET ? NUM_PROXY : NUM_TARGET ))

cat <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
kubeadmConfigPatches:
  - |
    kind: KubeletConfiguration
    apiVersion: kubelet.config.k8s.io/v1beta1
    featureGates:
      KubeletInUserNamespace: true
    cgroupDriver: "cgroupfs"
nodes:
  - role: control-plane
EOF

for (( i=1; i<=NUM_WORKERS; i++ )); do
cat <<EOF
  - role: worker
    extraMounts:
      - hostPath: /ais/log
        containerPath: /ais/log
EOF
done