// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

func ParsePodSpec(spec []byte) (*corev1.Pod, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(spec, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pod spec: %v", err)
	}

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return nil, fmt.Errorf("expected pod spec, got: %s", kind)
	}
	if _, ok := pod.Labels["app"]; !ok {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels["app"] = pod.GetName() + "-app"
	}
	return pod, nil
}

func podTransformCommType(pod *corev1.Pod) (string, error) {
	if pod.Annotations == nil || pod.Annotations["communication_type"] == "" {
		// By default assume `PushCommType`.
		return PushCommType, nil
	}

	commType := pod.Annotations["communication_type"]
	return commType, validateCommType(commType)
}

func validateCommType(commType string) error {
	if !cmn.StringInSlice(commType, []string{PushCommType, RedirectCommType, RevProxyCommType}) {
		return fmt.Errorf("unknown communication type: %q", commType)
	}
	return nil
}

func podTransformTimeout(pod *corev1.Pod) (cmn.DurationJSON, error) {
	if pod.Annotations == nil || pod.Annotations["wait_timeout"] == "" {
		return 0, nil
	}

	v, err := time.ParseDuration(pod.Annotations["wait_timeout"])
	return cmn.DurationJSON(v), err
}

func ValidateSpec(spec []byte) (msg Msg, err error) {
	msg.Spec = spec
	pod, err := ParsePodSpec(msg.Spec)
	if err != nil {
		return msg, err
	}

	// Check pod specification constraints.
	if len(pod.Spec.Containers) != 1 {
		// TODO: we could allow more but we would need to enforce that the "main" one is specified first.
		return msg, fmt.Errorf("unsupported number of containers (%d), expected: 1", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	if len(container.Ports) != 1 {
		return msg, fmt.Errorf("unsupported number of container ports (%d), expected: 1", len(container.Ports))
	}

	// Check annotations.
	if msg.CommType, err = podTransformCommType(pod); err != nil {
		return msg, err
	}
	if msg.WaitTimeout, err = podTransformTimeout(pod); err != nil {
		return msg, err
	}
	return msg, nil
}
