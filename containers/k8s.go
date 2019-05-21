// Package containers provides common utilities for managing containerized deployments of AIS
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package containers

import (
	"fmt"
	"os/exec"

	jsoniter "github.com/json-iterator/go"
)

// Data returned by kubectl - only fields used by AIS are mentioned
type (
	k8sPodList struct {
		Items []*k8sPod `json:"items"`
	}
	k8sPod struct {
		Meta   k8sPodMeta   `json:"metadata"`
		Spec   k8sPodSpec   `json:"spec"`
		Status k8sPodStatus `json:"status"`
	}
	k8sPodMeta struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	}
	k8sPodSpec struct {
		Containers []*k8sContainer `json:"containers"`
	}
	k8sContainer struct {
		Ports []*k8sPort `json:"ports"`
	}
	k8sPort struct {
		Port int `json:"containerPort"`
	}
	k8sPodStatus struct {
		IP string `json:"podIP"`
	}
)

const (
	proxySelector     = "--selector=app=ais,component=proxy"
	primaryLabel      = "initial_primary_proxy"
	primaryLabelValue = "yes"
	namespaceLabel    = "--namespace="
)

// K8sPrimaryURL returns URL of a primary proxy in a K8S cluster
// 1. Read all available pods for a default cluster and filters only ones
//    which labels `app` is `ais`, and `component` is `proxy`
// 2. Namespace allows a user to select a proxy from a given namespace when
//    K8S has more than one cluster running
// 3. Look through the list of proxies and returns a proxy's IP that has
//    label `initial_primary_proxy=yes`
// 4. If proxy with that label is not found, it returns the first proxy's IP
func K8sPrimaryURL(namespace ...string) (string, error) {
	var cmd *exec.Cmd
	if len(namespace) == 0 || namespace[0] == "" {
		cmd = exec.Command("kubectl", "get", "pods", proxySelector, "-o", "json")
	} else {
		cmd = exec.Command("kubectl", "get", "pods", proxySelector, namespaceLabel+namespace[0], "-o", "json")
	}
	bt, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("Failed to read pod list: %v", err)
	}

	podList := &k8sPodList{}
	err = jsoniter.Unmarshal(bt, podList)
	if err != nil {
		return "", fmt.Errorf("Failed to unmarshal pod list: %v", err)
	}

	proxyURL := ""
	for _, pod := range podList.Items {
		if len(pod.Spec.Containers) == 0 {
			continue
		}
		port := pod.Spec.Containers[0].Ports[0].Port
		if val, ok := pod.Meta.Labels[primaryLabel]; ok && val == primaryLabelValue {
			url := fmt.Sprintf("http://%s:%d", pod.Status.IP, port)
			return url, nil
		}

		if proxyURL == "" {
			proxyURL = fmt.Sprintf("http://%s:%d", pod.Status.IP, port)
		}
	}

	if proxyURL != "" {
		return proxyURL, nil
	}

	return "", fmt.Errorf("Failed to detect primary proxy IP")
}
