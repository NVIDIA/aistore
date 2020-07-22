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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	targetsNodeName = os.Getenv("AIS_NODE_NAME")

	staticTransformers = map[string]struct{}{
		cmn.Tar2Tf: {},
	}
)

const (
	// built-in label https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels
	nodeNameLabel = "kubernetes.io/hostname"
)

// TODO: remove the `kubectl` with a proper go-sdk call
// If transformationName is empty, it will be replaced with name of a pod.
func StartTransformationPod(t cluster.Target, msg Msg, transformationName string) (err error) {
	// Parse spec template.
	pod, err := ParsePodSpec(msg.Spec)
	if err != nil {
		return err
	}

	if transformationName == "" {
		transformationName = pod.GetName()
		if IsStaticTransformer(transformationName) {
			return fmt.Errorf("can't start transformation with the same name as static transformation %q", transformationName)
		}
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

	// Retrieve host IP of the pod.
	output, err := exec.Command("kubectl", "get", "pod", pod.GetName(), "--template={{.status.hostIP}}").Output()
	if err != nil {
		return fmt.Errorf("failed to get IP of %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}
	hostIP := string(output)

	// Proxy the pod behind a service
	svcPod := getSvcPod(pod)

	// Encode the specification for the service.
	b, err = jsoniter.Marshal(svcPod)
	if err != nil {
		return err
	}

	// Creating the service
	cmd = exec.Command("kubectl", "replace", "--force", "-f", "-")
	cmd.Stdin = bytes.NewBuffer(b)
	if b, err = cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to apply spec for %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}

	// Retrieve NodePort of the service
	output, err = exec.Command("kubectl", "get", "-o", "jsonpath=\"{.spec.ports[0].nodePort}\"", "svc", svcPod.GetName()).Output()
	if err != nil {
		return fmt.Errorf("failed to get IP of %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}
	outputStr, _ := strconv.Unquote(string(output))
	nodePort, err := strconv.Atoi(outputStr)
	if err != nil {
		return fmt.Errorf("failed to parse nodePort for pod-svc %q (err: %v; output: %s)", svcPod.GetName(), err, string(output))
	}

	transformerURL := fmt.Sprintf("http://%s:%d", hostIP, nodePort)
	c := makeCommunicator(t, pod, msg.CommType, transformerURL, transformationName)
	reg.put(msg.ID, c)
	return nil
}

func getSvcPod(pod *corev1.Pod) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.GetName(),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Port: pod.Spec.Containers[0].Ports[0].ContainerPort},
			},
			Selector: map[string]string{
				"app": pod.Labels["app"],
			},
			Type: corev1.ServiceTypeNodePort,
		},
	}
}

func StopTransformationPod(id string) error {
	c, exists := reg.getByUUID(id)
	if !exists {
		return fmt.Errorf("transformation with %q id doesn't exist", id)
	}

	if IsStaticTransformer(c.Name()) {
		return fmt.Errorf("can't stop static transformation %q", id)
	}

	err := exec.Command("kubectl", "delete", "pod", c.PodName(), "--force").Run()
	if err != nil {
		return err
	}
	reg.removeByUUID(id)
	return nil
}

func GetCommunicator(transformID string) (Communicator, error) {
	c, exists := reg.getByUUID(transformID)
	if !exists {
		return nil, fmt.Errorf("transformation with %q id doesn't exist", transformID)
	}
	return c, nil
}

func GetCommunicatorByName(transformName string) (Communicator, error) {
	c, exists := reg.getByName(transformName)
	if !exists {
		return nil, fmt.Errorf("transformation with %q name doesn't exist", transformName)
	}
	return c, nil
}

func ListTransforms() []TransformationInfo { return reg.list() }

func IsStaticTransformer(name string) bool {
	_, ok := staticTransformers[name]
	return ok
}

// This is called for cleanup after end of execution
func StopStaticTransformers() {
	for staticTf := range staticTransformers {
		c, exists := reg.getByName(staticTf)
		if !exists {
			glog.Errorf("transformation with name %q id doesn't exist", staticTf)
			continue
		}
		err := exec.Command("kubectl", "delete", "pod", c.PodName(), "--grace-period=5").Run()
		if err != nil {
			glog.Errorf("error deleting static transformer: %s, err: %v", staticTf, err)
		}
	}
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
