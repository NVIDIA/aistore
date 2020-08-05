// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"os"
	"os/exec"
)

var (
	targetsNodeName = os.Getenv("K8S_HOST_NAME")
	isKube          = false
)

func InitKubernetes() {
	output, err := exec.Command(Kubectl, "get", "node", targetsNodeName, "--template={{.metadata.name}}").Output()
	if err != nil {
		return
	}
	if string(output) != targetsNodeName {
		return
	}
	isKube = true
}

func GetKubernetesNodeName() string {
	return targetsNodeName
}

func CheckKubernetesDeployment() error {
	if !isKube {
		return fmt.Errorf("operation only supported with kubernetes deployment")
	}
	return nil
}
