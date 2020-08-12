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

// Currently we need the `default` port (on which the application runs) to be same as the
// `readiness` probe port in the pod spec.
func ParsePodSpec(errCtx *cmn.ETLErrorContext, spec []byte) (*corev1.Pod, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(spec, nil, nil)
	if err != nil {
		return nil, cmn.NewETLError(errCtx, "failed to parse pod spec: %v", err)
	}

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return nil, cmn.NewETLError(errCtx, "expected pod spec, got: %s", kind)
	}
	if _, ok := pod.Labels["app"]; !ok {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels["app"] = pod.GetName() + "-app"
	}
	return pod, nil
}

func podTransformCommType(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	if pod.Annotations == nil || pod.Annotations["communication_type"] == "" {
		// By default assume `PushCommType`.
		return PushCommType, nil
	}

	commType := pod.Annotations["communication_type"]
	if err := validateCommType(commType); err != nil {
		return "", cmn.NewETLError(errCtx, err.Error()).WithPodName(pod.Name)
	}
	return commType, nil
}

func validateCommType(commType string) error {
	if !cmn.StringInSlice(commType, []string{PushCommType, RedirectCommType, RevProxyCommType}) {
		return fmt.Errorf("unknown communication type: %q", commType)
	}
	return nil
}

func podTransformTimeout(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (cmn.DurationJSON, error) {
	if pod.Annotations == nil || pod.Annotations["wait_timeout"] == "" {
		return 0, nil
	}

	v, err := time.ParseDuration(pod.Annotations["wait_timeout"])
	if err != nil {
		return cmn.DurationJSON(v), cmn.NewETLError(errCtx, err.Error()).WithPodName(pod.Name)
	}
	return cmn.DurationJSON(v), nil
}

func ValidateSpec(spec []byte) (msg Msg, err error) {
	var (
		errCtx = &cmn.ETLErrorContext{}
	)
	msg.Spec = spec
	pod, err := ParsePodSpec(errCtx, msg.Spec)
	if err != nil {
		return msg, err
	}
	errCtx.ETLName = pod.GetName()

	// Check pod specification constraints.
	if len(pod.Spec.Containers) != 1 {
		// TODO: we could allow more but we would need to enforce that the "main" one is specified first.
		return msg, cmn.NewETLError(errCtx, "unsupported number of containers (%d), expected: 1", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	if len(container.Ports) != 1 {
		return msg, cmn.NewETLError(errCtx, "unsupported number of container ports (%d), expected: 1", len(container.Ports))
	}

	// Validate that user container supports health check.
	// Currently we need the `default` port (on which the application runs) to be same as the
	// `readiness` probe port.
	if container.Ports[0].Name != cmn.KubeDefault {
		return msg, cmn.NewETLError(errCtx, "expected port name %q got %q", cmn.KubeDefault, container.Ports[0].Name)
	}
	if container.ReadinessProbe == nil {
		return msg, cmn.NewETLError(errCtx, "readinessProbe is required in a container spec")
	}
	// TODO: add support for other healthchecks
	if container.ReadinessProbe.HTTPGet == nil {
		return msg, cmn.NewETLError(errCtx, "httpGet missing in the readinessProbe")
	}
	if container.ReadinessProbe.HTTPGet.Port.StrVal != cmn.KubeDefault {
		return msg, cmn.NewETLError(errCtx, "readinessProbe port must be the '%q' port", cmn.KubeDefault)
	}

	// Check annotations.
	if msg.CommType, err = podTransformCommType(errCtx, pod); err != nil {
		return msg, err
	}
	if msg.WaitTimeout, err = podTransformTimeout(errCtx, pod); err != nil {
		return msg, err
	}
	return msg, nil
}
