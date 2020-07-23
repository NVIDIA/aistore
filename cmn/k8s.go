// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"os"
)

var (
	targetsNodeName = os.Getenv("AIS_NODE_NAME")
)

func GetKubernetesNodeName() string {
	return targetsNodeName
}

func CheckKubernetesDeployment() error {
	// TODO: someone might define this env variable during deployment even if
	// it's not a kubernetes deployment. However, there doesn't seem to be any
	// 100% confident method of checking if it's a kubernetes deployment
	if targetsNodeName == "" {
		return fmt.Errorf("operation only supported with kubernetes deployment")
	}
	return nil
}
