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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	appLabel = "app"

	commTypeAnnotation    = "communication_type"
	waitTimeoutAnnotation = "wait_timeout"
)

type etlBootstraper struct {
	// These fields are set at the start.

	errCtx *cmn.ETLErrorContext
	t      cluster.Target
	msg    InitSpecMsg
	env    map[string]string

	// These fields are set during bootstrap.

	pod             *corev1.Pod
	svc             *corev1.Service
	uri             string
	originalPodName string
	originalCommand []string
}

func ParsePodSpec(errCtx *cmn.ETLErrorContext, spec []byte) (*corev1.Pod, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(spec, nil, nil)
	if err != nil {
		return nil, cmn.NewErrETL(errCtx, "failed to parse pod spec: %v", err)
	}

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return nil, cmn.NewErrETL(errCtx, "expected pod spec, got: %s", kind)
	}
	return pod, nil
}

func ValidateSpec(spec []byte) (msg InitSpecMsg, err error) {
	errCtx := &cmn.ETLErrorContext{}
	msg.Spec = spec
	pod, err := ParsePodSpec(errCtx, msg.Spec)
	if err != nil {
		return msg, err
	}
	errCtx.ETLName = pod.GetName()
	msg.ID = pod.GetName()

	if err := cos.ValidateID(msg.ID); err != nil {
		err = fmt.Errorf("pod name not in valid ID format, err: %v", err)
		return msg, err
	}

	// Check annotations.
	if msg.CommType, err = podTransformCommType(errCtx, pod); err != nil {
		return msg, err
	}
	if msg.WaitTimeout, err = podTransformTimeout(errCtx, pod); err != nil {
		return msg, err
	}

	// Check pod specification constraints.
	if len(pod.Spec.Containers) != 1 {
		return msg, cmn.NewErrETL(errCtx, "unsupported number of containers (%d), expected: 1", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	if len(container.Ports) != 1 {
		return msg, cmn.NewErrETL(errCtx, "unsupported number of container ports (%d), expected: 1", len(container.Ports))
	}
	if container.Ports[0].Name != k8s.Default {
		return msg, cmn.NewErrETL(errCtx, "expected port name: %q, got: %q", k8s.Default, container.Ports[0].Name)
	}

	if msg.CommType != IOCommType {
		// Validate that user container supports health check.
		// Currently we need the `default` port (on which the application runs) to
		// be same as the `readiness` probe port.
		if container.ReadinessProbe == nil {
			return msg, cmn.NewErrETL(errCtx, "readinessProbe section is required in a container spec")
		}
		// TODO: Add support for other health checks.
		if container.ReadinessProbe.HTTPGet == nil {
			return msg, cmn.NewErrETL(errCtx, "httpGet missing in the readinessProbe")
		}
		if container.ReadinessProbe.HTTPGet.Path == "" {
			return msg, cmn.NewErrETL(errCtx, "expected non-empty path for readinessProbe")
		}
		// Currently we need the `default` port (on which the application runs)
		// to be same as the `readiness` probe port in the pod spec.
		if container.ReadinessProbe.HTTPGet.Port.StrVal != k8s.Default {
			return msg, cmn.NewErrETL(errCtx, "readinessProbe port must be the %q port", k8s.Default)
		}
	}

	return msg, nil
}

func (b *etlBootstraper) preparePodSpec() (err error) {
	// Override the name (add target's daemon ID and node ID to its name).
	b.pod.SetName(k8s.CleanName(b.pod.GetName() + "-" + b.t.SID()))
	b.errCtx.PodName = b.pod.GetName()
	b.pod.APIVersion = "v1"

	// The following combination of Affinity and Anti-Affinity allows one to
	// achieve the following:
	//  1. The ETL container is always scheduled on the target invoking it.
	//  2. Not more than one ETL container with the same target, is scheduled on
	//     the same node, at a given point of time.
	if err = b.setTransformAffinity(); err != nil {
		return
	}
	if err = b.setTransformAntiAffinity(); err != nil {
		return
	}

	b.updatePodCommand()

	b.updatePodLabels()
	b.updateReadinessProbe()
	b.setPodEnvVariables()
	return
}

func (b *etlBootstraper) updatePodCommand() {
	if b.msg.CommType != IOCommType {
		return
	}

	b.originalCommand = b.pod.Spec.Containers[0].Command
	b.pod.Spec.Containers[0].Command = []string{"sh", "-c", "while true; do sleep 1000000; done"}
}

func (b *etlBootstraper) createPodSpec() (err error) {
	if b.pod, err = ParsePodSpec(b.errCtx, b.msg.Spec); err != nil {
		return
	}
	b.originalPodName = b.pod.GetName()
	b.errCtx.ETLName = b.originalPodName
	return b.preparePodSpec()
}

func podTransformCommType(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	if pod.Annotations == nil || pod.Annotations[commTypeAnnotation] == "" {
		// By default assume `PushCommType`.
		return PushCommType, nil
	}

	commType := pod.Annotations[commTypeAnnotation]
	if err := validateCommType(commType); err != nil {
		return "", cmn.NewErrETL(errCtx, err.Error()).WithPodName(pod.Name)
	}
	return commType, nil
}

func validateCommType(commType string) error {
	if !cos.StringInSlice(commType, commTypes) {
		return fmt.Errorf("unknown communication type: %q", commType)
	}
	return nil
}

func podTransformTimeout(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (cos.Duration, error) {
	if pod.Annotations == nil || pod.Annotations[waitTimeoutAnnotation] == "" {
		return 0, nil
	}

	v, err := time.ParseDuration(pod.Annotations[waitTimeoutAnnotation])
	if err != nil {
		return cos.Duration(v), cmn.NewErrETL(errCtx, err.Error()).WithPodName(pod.Name)
	}
	return cos.Duration(v), nil
}
