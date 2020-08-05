// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "os"

const (
	K8SHostName = "K8S_HOST_NAME"
)

func GetK8sNodeName() string {
	return os.Getenv(K8SHostName)
}
