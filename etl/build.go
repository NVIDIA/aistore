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
		// TODO: Maybe we should think about more standard naming that would be
		//  more meaningful to the user. Or allow user it set it.
		name    = "etl-" + strings.ToLower(cmn.RandString(5))
		podSpec = runtime.PodSpec()
		errCtx  = &cmn.ETLErrorContext{
			Tid:  t.Snode().DaemonID,
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

	// TODO: remove config map when initialization has been finished.

	// Finally, start the ETL with declared Pod specification.
	return Start(t, InitMsg{
		ID:       msg.ID,
		Spec:     []byte(podSpec),
		CommType: PushCommType,
	})
}
