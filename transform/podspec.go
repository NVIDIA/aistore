// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

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
	return pod, nil
}

func PodTransformCommType(pod *corev1.Pod) (string, error) {
	if pod.Annotations == nil || pod.Annotations["communication_type"] == "" {
		// By default assume `putCommType`
		return putCommType, nil
	}

	commType := pod.Annotations["communication_type"]
	return commType, validateCommType(commType)
}

func validateCommType(commType string) error {
	if !cmn.StringInSlice(commType, []string{putCommType, redirectCommType, revProxyCommType}) {
		return fmt.Errorf("unknown communication type: %q", commType)
	}
	return nil
}

func PodTransformTimeout(pod *corev1.Pod) (time.Duration, error) {
	if pod.Annotations == nil || pod.Annotations["wait_timeout"] == "" {
		return 0, nil
	}
	return time.ParseDuration(pod.Annotations["wait_timeout"])
}

func ValidateSpec(pod *corev1.Pod) error {
	if _, err := PodTransformCommType(pod); err != nil {
		return err
	}
	_, err := PodTransformTimeout(pod)
	return err
}
