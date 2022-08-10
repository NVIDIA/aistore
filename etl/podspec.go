// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	appLabel = "app"
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
	xctn            cluster.Xact
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

func (b *etlBootstraper) preparePodSpec() (err error) {
	// Override the name (add target's daemon ID and node ID to its name).
	b.pod.SetName(k8s.CleanName(b.msg.IDX + "-" + b.t.SID()))
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
	if b.msg.CommTypeX != HpushStdin {
		return
	}

	b.originalCommand = b.pod.Spec.Containers[0].Command
	b.pod.Spec.Containers[0].Command = []string{"sh", "-c", "/server"}
}

func (b *etlBootstraper) createPodSpec() (err error) {
	if b.pod, err = ParsePodSpec(b.errCtx, b.msg.Spec); err != nil {
		return
	}
	b.originalPodName = b.pod.GetName()
	b.errCtx.ETLName = b.originalPodName
	return b.preparePodSpec()
}

func validateCommType(commType string) error {
	if commType != "" && !cos.StringInSlice(commType, commTypes) {
		return fmt.Errorf("unknown communication type: %q", commType)
	}
	return nil
}
