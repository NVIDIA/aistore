// Package main contains logic for the aisinit container
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	aisapc "github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	aiscmn "github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"

	jsoniter "github.com/json-iterator/go"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	defaultClusterDomain = "cluster.local"
	retryInterval        = 10 * time.Second
)

func getRequiredEnv(envVar string) string {
	val := strings.TrimSpace(os.Getenv(envVar))
	failOnCondition(val != "", "env %q is required!", envVar)
	return val
}

func getOrDefaultEnv(envVar, defaultVal string) string {
	val := strings.TrimSpace(os.Getenv(envVar))
	if val != "" {
		return val
	}
	return defaultVal
}

func failOnCondition(cond bool, msg string, a ...any) {
	if cond {
		return
	}
	nlog.Errorf(msg, a...)
	os.Exit(1)
}

func failOnError(err error) {
	if err == nil {
		return
	}
	nlog.Errorln(err)
	os.Exit(1)
}

func fetchExternalIP(svcName, namespace string) string {
	config, err := rest.InClusterConfig()
	failOnError(err)
	client := kubernetes.NewForConfigOrDie(config)
	for i := 1; i <= 6; i++ {
		externalService, err := client.CoreV1().Services(namespace).Get(context.TODO(), svcName, metav1.GetOptions{})
		if err != nil && errors.IsNotFound(err) {
			failOnError(err)
		}

		if err == nil && len(externalService.Status.LoadBalancer.Ingress) > 0 {
			return externalService.Status.LoadBalancer.Ingress[0].IP
		}
		nlog.Warningf("couldn't fetch valid external loadbalancer IP for svc %q, attempt: %d", svcName, i)
		time.Sleep(retryInterval)
	}
	failOnCondition(false, "Failed to fetch external IP for target")
	return ""
}

func getMappedHostname(original, mapPath string) string {
	data, err := os.ReadFile(mapPath)
	failOnError(err)
	var hostmap map[string]string
	err = jsoniter.Unmarshal(data, &hostmap)
	failOnError(err)
	if mapped, ok := hostmap[original]; ok && strings.TrimSpace(mapped) != "" {
		return mapped
	}
	return original
}

// AIS init is intended to be deployed inside the same pod as the aisnode container
// The purpose of AIS init is to take a template config and output a result config based on the environment of the
// deployed pod, with optional additional flags
func main() {
	var (
		role                     string
		aisLocalConfigTemplate   string
		aisClusterConfigOverride string

		outputClusterConfig string
		outputLocalConfig   string
		hostnameMapFile     string

		localConf aiscmn.LocalConfig
	)

	flag.StringVar(&role, "role", "", "AISNode role")
	flag.StringVar(&aisLocalConfigTemplate, "local_config_template", "", "local template file path")
	flag.StringVar(&outputLocalConfig, "output_local_config", "", "output local config path")
	flag.StringVar(&aisClusterConfigOverride, "cluster_config_override", "", "cluster config override file path")
	flag.StringVar(&outputClusterConfig, "output_cluster_config", "", "output cluster config path")
	flag.StringVar(&hostnameMapFile, "hostname_map_file", "", "path to file containing hostname map")
	flag.Parse()

	failOnCondition(role == aisapc.Proxy || role == aisapc.Target, "invalid role provided %q", role)

	confBytes, err := os.ReadFile(aisLocalConfigTemplate)
	failOnError(err)
	err = json.Unmarshal(confBytes, &localConf)
	failOnError(err)

	namespace := getRequiredEnv(env.AisK8sNamespace)
	serviceName := getRequiredEnv(env.AisK8sServiceName)
	podName := getRequiredEnv(env.AisK8sPod)
	clusterDomain := getOrDefaultEnv(env.AisK8sClusterDomain, defaultClusterDomain)
	publicHostName := getOrDefaultEnv(env.AisK8sPublicHostname, "")
	podDNS := fmt.Sprintf("%s.%s.%s.svc.%s", podName, serviceName, namespace, clusterDomain)

	localConf.HostNet.HostnameIntraControl = podDNS
	localConf.HostNet.HostnameIntraData = podDNS
	localConf.HostNet.Hostname = publicHostName

	if role == aisapc.Target {
		useHostNetwork, err := cos.ParseBool(getOrDefaultEnv(env.AisK8sHostNetwork, "false"))
		failOnError(err)
		if useHostNetwork {
			localConf.HostNet.HostnameIntraData = ""
			localConf.HostNet.Hostname = ""
		}

		useExternalLB, err := cos.ParseBool(getOrDefaultEnv(env.AisK8sEnableExternalAccess, "false"))
		failOnError(err)
		if useExternalLB {
			localConf.HostNet.Hostname = fetchExternalIP(podName, namespace)
		}
	}

	if hostnameMapFile != "" {
		localConf.HostNet.Hostname = getMappedHostname(localConf.HostNet.Hostname, hostnameMapFile)
	}

	data, err := jsoniter.Marshal(localConf)
	failOnError(err)
	err = os.WriteFile(outputLocalConfig, data, 0o644) //nolint:gosec // For now we don't want to change this.
	failOnError(err)

	populateClusterConfig(aisClusterConfigOverride, outputClusterConfig)
}

func populateClusterConfig(aisClusterConfigOverride, outputClusterConfig string) {
	if outputClusterConfig == "" || aisClusterConfigOverride == "" {
		return
	}
	defaultConfig := newDefaultConfig()
	data, err := os.ReadFile(aisClusterConfigOverride)
	failOnError(err)

	var configOverride aiscmn.ConfigToSet
	err = json.Unmarshal(data, &configOverride)
	failOnError(err)

	err = aiscmn.CopyProps(configOverride, defaultConfig, aisapc.Cluster)
	failOnError(err)

	data, err = jsoniter.Marshal(defaultConfig)
	failOnError(err)

	err = os.WriteFile(outputClusterConfig, data, 0o644) //nolint:gosec // For now we don't want to change this.
	failOnError(err)
}
