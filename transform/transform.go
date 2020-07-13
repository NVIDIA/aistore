// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
	corev1 "k8s.io/api/core/v1"
)

var (
	targetsNodeName = os.Getenv("AIS_NODE_NAME")
)

const (
	// built-in label https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels
	nodeNameLabel = "kubernetes.io/hostname"
)

// TODO: remove the `kubectl` with a proper go-sdk call

func StartTransformationPod(t cluster.Target, msg Msg) (err error) {
	// Parse spec template.
	pod, err := ParsePodSpec(msg.Spec)
	if err != nil {
		return err
	}

	if targetsNodeName == "" {
		// Override the name (add target's daemon ID to its name).
		// Don't add any affinities if target's node name is unknown.
		pod.SetName(pod.GetName() + "-" + t.Snode().DaemonID)
		glog.Warningf("transformation %q starting without node affinity", pod.GetName())
	} else {
		// Override the name (add target's daemon ID and node ID to its name).
		pod.SetName(pod.GetName() + "-" + t.Snode().DaemonID + "-" + targetsNodeName)
		if err := setTransformAffinity(pod); err != nil {
			return err
		}
	}

	if err := setPodEnvVariables(pod, t); err != nil {
		return err
	}

	// Encode the specification once again to be ready for the start.
	b, err := jsoniter.Marshal(pod)
	if err != nil {
		return err
	}

	// Start the pod.
	cmd := exec.Command("kubectl", "replace", "--force", "-f", "-")
	cmd.Stdin = bytes.NewBuffer(b)
	if b, err = cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to apply spec for %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}

	// Wait for the pod to start.
	args := []string{"wait"}
	if !msg.WaitTimeout.IsZero() {
		args = append(args, "--timeout", msg.WaitTimeout.String())
	}
	args = append(args, "--for", "condition=ready", "pod", pod.GetName())
	cmd = exec.Command("kubectl", args...)
	if b, err = cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to wait for %q pod to be ready (err: %v; output: %s)", pod.GetName(), err, string(b))
	}

	// Retrieve IP of the pod.
	output, err := exec.Command("kubectl", "get", "pod", pod.GetName(), "--template={{.status.podIP}}").Output()
	if err != nil {
		return fmt.Errorf("failed to get IP of %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}
	ip := string(output)

	// Retrieve port of the pod.
	p := pod.Spec.Containers[0].Ports[0].ContainerPort
	port := strconv.FormatInt(int64(p), 10)

	cmn.AssertNoErr(validateCommType(msg.CommType))
	transformerURL := fmt.Sprintf("http://%s:%s", ip, port)
	c := makeCommunicator(msg.CommType, transformerURL, t)
	reg.put(msg.ID, c)
	return nil
}

func GetCommunicator(transformID string) (Communicator, error) {
	c, exists := reg.get(transformID)
	if !exists {
		return nil, fmt.Errorf("transformation with %q id doesn't exist", transformID)
	}
	return c, nil
}

// Sets pods node affinity, so pod will be scheduled on the same node as a target creating it.
func setTransformAffinity(pod *corev1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	reqAffinity := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAffinity := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if reqAffinity != nil && len(reqAffinity.NodeSelectorTerms) > 0 || len(prefAffinity) > 0 {
		return fmt.Errorf("pod spec should not have any NodeAffinities defined")
	}

	nodeSelector := &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      nodeNameLabel,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{targetsNodeName},
			}}},
		},
	}
	// TODO: RequiredDuringSchedulingIgnoredDuringExecution means that transformer
	//  will be placed on the same machine as target which creates it. However,
	//  if 2 targets went down and up again at the same time, they may switch nodes,
	//  leaving transformers running on the wrong machines.
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return nil
}

// Sets environment variables that can be accessed inside the container.
func setPodEnvVariables(pod *corev1.Pod, t cluster.Target) error {
	containers := pod.Spec.Containers
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: t.Snode().URL(cmn.NetworkPublic),
		})
	}
	return nil
}
