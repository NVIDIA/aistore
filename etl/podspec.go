// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	appLabel = "app"

	commTypeAnnotation    = "communication_type"
	waitTimeoutAnnotation = "wait_timeout"
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
	return pod, nil
}

func preparePodSpec(errCtx *cmn.ETLErrorContext, t cluster.Target, pod *corev1.Pod, env map[string]string) (err error) {
	// Override the name (add target's daemon ID and node ID to its name).
	pod.SetName(k8s.CleanName(pod.GetName() + "-" + t.Snode().ID()))
	errCtx.PodName = pod.GetName()

	// The following combination of Affinity and Anti-Affinity allows one to
	// achieve the following:
	//  1. The ETL container is always scheduled on the target invoking it.
	//  2. Not more than one ETL container with the same target, is scheduled on
	//     the same node, at a given point of time.
	if err = setTransformAffinity(errCtx, pod); err != nil {
		return
	}
	if err = setTransformAntiAffinity(errCtx, pod); err != nil {
		return
	}

	updatePodLabels(t, pod)
	updateReadinessProbe(pod)
	setPodEnvVariables(t, pod, env)
	return
}

func createPodSpec(errCtx *cmn.ETLErrorContext, t cluster.Target, spec []byte, env map[string]string) (pod *corev1.Pod, orgName string, err error) {
	if pod, err = ParsePodSpec(errCtx, spec); err != nil {
		return
	}
	orgName = pod.GetName()
	errCtx.ETLName = orgName
	return pod, orgName, preparePodSpec(errCtx, t, pod, env)
}

func podTransformCommType(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	if pod.Annotations == nil || pod.Annotations[commTypeAnnotation] == "" {
		// By default assume `PushCommType`.
		return PushCommType, nil
	}

	commType := pod.Annotations[commTypeAnnotation]
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
	if pod.Annotations == nil || pod.Annotations[waitTimeoutAnnotation] == "" {
		return 0, nil
	}

	v, err := time.ParseDuration(pod.Annotations[waitTimeoutAnnotation])
	if err != nil {
		return cmn.DurationJSON(v), cmn.NewETLError(errCtx, err.Error()).WithPodName(pod.Name)
	}
	return cmn.DurationJSON(v), nil
}

func ValidateSpec(spec []byte) (msg InitMsg, err error) {
	errCtx := &cmn.ETLErrorContext{}
	msg.Spec = spec
	pod, err := ParsePodSpec(errCtx, msg.Spec)
	if err != nil {
		return msg, err
	}
	errCtx.ETLName = pod.GetName()
	msg.ID = pod.GetName()

	if err := cmn.ValidateID(msg.ID); err != nil {
		err = fmt.Errorf("pod name not in valid ID format, err: %v", err)
		return msg, err
	}

	// Check pod specification constraints.
	if len(pod.Spec.Containers) != 1 {
		return msg, cmn.NewETLError(errCtx, "unsupported number of containers (%d), expected: 1", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	if len(container.Ports) != 1 {
		return msg, cmn.NewETLError(errCtx, "unsupported number of container ports (%d), expected: 1", len(container.Ports))
	}
	if container.Ports[0].Name != k8s.Default {
		return msg, cmn.NewETLError(errCtx, "expected port name: %q, got: %q", k8s.Default, container.Ports[0].Name)
	}

	// Validate that user container supports health check.
	// Currently we need the `default` port (on which the application runs) to
	// be same as the `readiness` probe port.
	if container.ReadinessProbe == nil {
		return msg, cmn.NewETLError(errCtx, "readinessProbe section is required in a container spec")
	}
	// TODO: Add support for other health checks.
	if container.ReadinessProbe.HTTPGet == nil {
		return msg, cmn.NewETLError(errCtx, "httpGet missing in the readinessProbe")
	}
	if container.ReadinessProbe.HTTPGet.Path == "" {
		return msg, cmn.NewETLError(errCtx, "expected non-empty path for readinessProbe")
	}
	if container.ReadinessProbe.HTTPGet.Port.StrVal != k8s.Default {
		return msg, cmn.NewETLError(errCtx, "readinessProbe port must be the %q port", k8s.Default)
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
