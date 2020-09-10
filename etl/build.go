// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/etl/template"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Build(t cluster.Target, msg BuildMsg) error {
	// Initialize runtime.
	runtime, exists := template.Runtimes[msg.Runtime]
	cmn.Assert(exists) // Runtime should be checked in proxy during validation.

	var (
		// We clean up the `msg.ID` as K8s doesn't allow `_` and uppercase
		// letters in the names.
		name    = "etl-" + strings.ReplaceAll(strings.ToLower(msg.ID), "_", "-")
		podSpec = runtime.PodSpec()
		errCtx  = &cmn.ETLErrorContext{
			TID:  t.Snode().DaemonID,
			UUID: msg.ID,
		}
	)
	podSpec = strings.ReplaceAll(podSpec, "<NAME>", name)
	podSpec = strings.Replace(podSpec, "<COMMUNICATION_TYPE>", PushCommType, 1)

	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name + "-source",
		},
		Data: map[string]string{
			runtime.CodeFileName(): string(msg.Code),
			runtime.DepsFileName(): string(msg.Deps),
		},
	}

	// Explicitly ignore error as multiple targets will try to create config map.
	// TODO: This entity should be created just once but selecting specific
	//  target will not work because some other target could be faster and
	//  could fail when creating a pod (config map is required to create a pod).
	_ = createEntity(errCtx, cmn.KubeConfigMap, configMap)

	// Finally, start the ETL with declared Pod specification.
	return Start(t, InitMsg{
		ID:       msg.ID,
		Spec:     []byte(podSpec),
		CommType: PushCommType,
	}, StartOpts{ConfigMapName: configMap.Name})
}
