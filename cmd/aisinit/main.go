// Package main contains logic for the aisinit container
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"context"
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

func retry[T any](what string, maxAttempts int, interval time.Duration, check func() (T, error)) (T, error) {
	var (
		val T
		err error
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if val, err = check(); err == nil {
			return val, nil
		}
		nlog.Warningf("couldn't %s, attempt: %d, error: %v", what, attempt, err)
		if attempt != maxAttempts {
			time.Sleep(interval)
		}
	}
	return val, err
}

func loadBalancerIP(client kubernetes.Interface, svcName, namespace string) (string, error) {
	svc, err := client.CoreV1().Services(namespace).Get(context.TODO(), svcName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return "", fmt.Errorf("svc %q has no loadbalancer ingress", svcName)
	}
	return svc.Status.LoadBalancer.Ingress[0].IP, nil
}

type hostNetParams struct {
	k8s            kubernetes.Interface // optional; defaults to the in-cluster client
	podName        string
	namespace      string
	serviceName    string
	clusterDomain  string
	publicHostName string
	override       string
	dnsMode        string
	isTarget       bool
	hostNetwork    bool
	externalAccess bool
}

func (p *hostNetParams) podDNS() string {
	return fmt.Sprintf("%s.%s.%s.svc.%s", p.podName, p.serviceName, p.namespace, p.clusterDomain)
}

// externalIP returns the LoadBalancer IP of the target's Service. It exits the process on failure.
func (p *hostNetParams) externalIP() string {
	if p.k8s == nil {
		config, err := rest.InClusterConfig()
		failOnError(err)
		p.k8s = kubernetes.NewForConfigOrDie(config)
	}
	svcName := p.podName
	what := fmt.Sprintf("fetch external loadbalancer IP for svc %q", svcName)
	ip, err := retry(what, 6, retryInterval, func() (string, error) {
		return loadBalancerIP(p.k8s, svcName, p.namespace)
	})
	failOnError(err)
	return ip
}

// With host networking in IP mode, aisnode discovers the target's own IP at startup.
func (p *hostNetParams) discoversIP() bool {
	return p.isTarget && p.hostNetwork && p.dnsMode == env.PubNetDNSModeIP
}

// Order: explicit override, LoadBalancer IP (target with external access),
// runtime discovery (empty), then the DNS mode.
func (p *hostNetParams) resolvePublicHostname() string {
	switch {
	case p.override != "":
		return p.override
	case p.isTarget && p.externalAccess:
		return p.externalIP()
	case p.discoversIP():
		return ""
	case p.dnsMode == env.PubNetDNSModePod:
		return p.podDNS()
	case p.dnsMode != env.PubNetDNSModeNode && p.dnsMode != env.PubNetDNSModeIP:
		// Fall back to publicHostName; if it is empty then aisnode will discover IP at startup.
		// We do not error here to preserve backwards compatibility for deployments that are using
		// ais-init:latest without a newer version of the ais-operator.
		nlog.Warningf("unknown %s %q, defaulting to '%s' for Hostname", env.AisK8sPublicDNSMode, p.dnsMode, p.publicHostName)
	}
	return p.publicHostName
}

func (p *hostNetParams) apply(hostNet *aiscmn.LocalNetConfig) {
	podDNS := p.podDNS()
	hostNet.HostnameIntraControl = podDNS
	hostNet.HostnameIntraData = podDNS
	if p.discoversIP() {
		hostNet.HostnameIntraData = ""
	}
	hostNet.Hostname = p.resolvePublicHostname()
}

// configureHostNet sets the hostnames in hostNet from the pod environment and the optional hostname map.
func configureHostNet(hostNet *aiscmn.LocalNetConfig, role, hostnameMapFile string) {
	params := hostNetParams{
		podName:        getRequiredEnv(env.AisK8sPod),
		namespace:      getRequiredEnv(env.AisK8sNamespace),
		serviceName:    getRequiredEnv(env.AisK8sServiceName),
		clusterDomain:  getOrDefaultEnv(env.AisK8sClusterDomain, defaultClusterDomain),
		publicHostName: getOrDefaultEnv(env.AisK8sPublicHostname, ""),
		override:       getOrDefaultEnv(env.AisK8sPublicHostnameOverride, ""),
		dnsMode:        getOrDefaultEnv(env.AisK8sPublicDNSMode, env.PubNetDNSModeIP),
		isTarget:       role == aisapc.Target,
	}
	if params.isTarget {
		var err error
		params.hostNetwork, err = cos.ParseBool(getOrDefaultEnv(env.AisK8sHostNetwork, "false"))
		failOnError(err)
		params.externalAccess, err = cos.ParseBool(getOrDefaultEnv(env.AisK8sEnableExternalAccess, "false"))
		failOnError(err)
	}

	params.apply(hostNet)
	if hostnameMapFile != "" {
		hostNet.Hostname = getMappedHostname(hostNet.Hostname, hostnameMapFile)
	}
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
	err = jsoniter.Unmarshal(confBytes, &localConf)
	failOnError(err)

	configureHostNet(&localConf.HostNet, role, hostnameMapFile)

	data, err := jsoniter.Marshal(localConf)
	failOnError(err)
	err = os.WriteFile(outputLocalConfig, data, 0o644) //nolint:gosec // For now we don't want to change this.
	failOnError(err)

	populateClusterConfig(aisClusterConfigOverride, outputClusterConfig)
}

// Generate the initial (bootstrap) cluster configuration: production defaults
// from config.go plus the deployment's ConfigToSet override.
//
// The flow:
// hydrate => partial override => validate/hydrate => sanitize => prune => write
//
// Read cmd/aisinit/README.md before changing this flow. The result is written
// in the same sparse form that aisnode expects for v5.0 configuration.
func populateClusterConfig(aisClusterConfigOverride, outputClusterConfig string) {
	if outputClusterConfig == "" || aisClusterConfigOverride == "" {
		return
	}

	confToWrite := newDefaultConfig()

	// Materialize canonical section defaults before applying a partial ConfigToSet override.
	err := confToWrite.HydrateOmittables()
	failOnError(err)

	data, err := os.ReadFile(aisClusterConfigOverride)
	failOnError(err)

	var configOverride aiscmn.ConfigToSet
	err = jsoniter.Unmarshal(data, &configOverride)
	failOnError(err)

	err = aiscmn.CopyProps(&configOverride, confToWrite, aisapc.Cluster)
	failOnError(err)

	// Validate and re-canonicalize the merged default-omittable sections.
	err = confToWrite.HydrateOmittables()
	failOnError(err)

	// Private secrets must be loaded through environment variables.
	confToWrite.Auth = confToWrite.Auth.PublicClone()

	// Drop sections that remain wholly default.
	confToWrite.PruneOmittables()

	data, err = jsoniter.Marshal(confToWrite)
	failOnError(err)

	err = os.WriteFile(outputClusterConfig, data, 0o644) //nolint:gosec // For now we don't want to change this.
	failOnError(err)
}
