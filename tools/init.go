// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/tlog"
)

const (
	defaultProxyURL = "http://localhost:8080"      // the url for the cluster's proxy (local)
	dockerEnvFile   = "/tmp/docker_ais/deploy.env" // filepath of Docker deployment config
)

const (
	registerTimeout = time.Minute * 2
)

type (
	// command used to restore a node
	RestoreCmd struct {
		Node *meta.Snode
		Cmd  string
		Args []string
		PID  int
	}
	ClusterType string
)

// Cluster type used for test
const (
	ClusterTypeLocal  ClusterType = "local"
	ClusterTypeDocker ClusterType = "docker"
	ClusterTypeK8s    ClusterType = "k8s"
)

type g struct {
	Client *http.Client
}

var (
	proxyURLReadOnly string       // user-defined primary proxy URL - it is read-only variable and tests mustn't change it
	pmapReadOnly     meta.NodeMap // initial proxy map - it is read-only variable
	testClusterType  ClusterType  // AIS cluster type - it is read-only variable

	currSmap *meta.Smap

	restoreNodesOnce sync.Once             // Ensures that the initialization happens only once.
	restoreNodes     map[string]RestoreCmd // initial proxy and target nodes => command to restore them

	transportArgs = cmn.TransportArgs{
		Timeout:         600 * time.Second,
		UseHTTPProxyEnv: true,

		// Allow a lot of idle connections so they can be reused when making huge
		// number of requests (eg. in `TestETLBigBucket`).
		MaxIdleConns:     2000,
		IdleConnsPerHost: 200,
	}
	tlsArgs = cmn.TLSArgs{
		SkipVerify: true,
	}

	RemoteCluster struct {
		UUID  string
		Alias string
		URL   string
	}
	LoggedUserToken string

	gctx g
)

// NOTE:
// With no access to cluster configuration the tests
// currently simply detect protocol type by the env.AisEndpoint (proxy's) URL.
// Certificate check and other TLS is always disabled.

func init() {
	if cos.IsHTTPS(os.Getenv(env.AisEndpoint)) {
		// fill-in from env
		cmn.EnvToTLS(&tlsArgs)
		gctx.Client = cmn.NewClientTLS(transportArgs, tlsArgs, false /*intra-cluster*/)
	} else {
		gctx.Client = cmn.NewClient(transportArgs)
	}
}

func NewClientWithProxy(proxyURL string) *http.Client {
	var (
		transport      = cmn.NewTransport(transportArgs)
		parsedURL, err = url.Parse(proxyURL)
	)
	cos.AssertNoErr(err)
	transport.Proxy = http.ProxyURL(parsedURL)

	if parsedURL.Scheme == "https" {
		cos.AssertMsg(cos.IsHTTPS(proxyURL), proxyURL)
		tlsConfig, err := cmn.NewTLS(tlsArgs, false /*intra-cluster*/)
		cos.AssertNoErr(err)
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{
		Transport: transport,
		Timeout:   transportArgs.Timeout,
	}
}

// InitLocalCluster initializes AIS cluster that must be either:
//  1. deployed locally using `make deploy` command and accessible @ localhost:8080, or
//  2. deployed in local docker environment, or
//  3. provided via `AIS_ENDPOINT` environment variable
//
// In addition, try to query remote AIS cluster that may or may not be locally deployed as well.
func InitLocalCluster() {
	var (
		// Gets the fields from the .env file from which the docker was deployed
		envVars = parseEnvVariables(dockerEnvFile)
		// Host IP and port of primary cluster
		primaryHostIP, port = envVars["PRIMARY_HOST_IP"], envVars["PORT"]

		clusterType = ClusterTypeLocal
		proxyURL    = defaultProxyURL
	)

	// FIXME:
	// - hardcoded schema
	// - use cmn.HostPort instead
	if docker.IsRunning() {
		clusterType = ClusterTypeDocker
		proxyURL = "http://" + primaryHostIP + ":" + port
	}

	if v := os.Getenv(env.AisUseIPv6); cos.IsParseBool(v) {
		transportArgs.UseIPv6 = true
	}

	// This is needed for testing on Kubernetes if we want to run 'make test-XXX'
	// Many of the other packages do not accept the 'url' flag
	if cliAISURL := os.Getenv(env.AisEndpoint); cliAISURL != "" {
		if !strings.HasPrefix(cliAISURL, "http") {
			cliAISURL = "http://" + cliAISURL
		}
		proxyURL = cliAISURL
	}

	err := InitCluster(proxyURL, clusterType)
	if err == nil {
		initRemAis() // remote AIS that optionally may be run locally as well and used for testing
		return
	}
	tlog.Logfln("Error: %s\n", strings.TrimSuffix(err.Error(), "\n"))

	switch {
	case strings.Contains(err.Error(), "token"):
		tlog.Logfln("Hint: make sure to provide access token via %s environment or the default config location",
			env.AisAuthTokenFile)
	case strings.Contains(err.Error(), "unreachable"):
		tlog.Logfln("Hint: make sure that cluster is running and/or specify its endpoint via %s environment",
			env.AisEndpoint)
	default:
		tlog.Logfln("Hint: check api/env/*.go environment and, in particular %s=%s", env.AisEndpoint, os.Getenv(env.AisEndpoint))
		if len(envVars) > 0 {
			fmt.Println("Docker Environment:")
			for k, v := range envVars {
				tlog.Logfln("\t%s:\t%s", k, v)
			}
		}
	}

	os.Exit(1)
}

// InitCluster initializes the environment necessary for testing against an AIS cluster.
// NOTE: the function is also used for testing by NVIDIA/ais-k8s Operator
func InitCluster(proxyURL string, clusterType ClusterType) (err error) {
	LoggedUserToken, _ = authn.LoadToken("") // ignore error as not all tests require token
	proxyURLReadOnly = proxyURL
	testClusterType = clusterType
	if err = initProxyURL(); err != nil {
		return
	}
	return initPmap()
}

func initProxyURL() error {
	args := &cmn.RetryArgs{
		Call:     func() (int, error) { return 0, GetProxyReadiness(proxyURLReadOnly) },
		SoftErr:  5,
		HardErr:  5,
		Sleep:    5 * time.Second,
		Action:   "reach AIS at " + proxyURLReadOnly,
		IsClient: true,
	}
	ecode, err := args.Do()
	if err != nil {
		err = errors.New("AIS is unreachable at " + proxyURLReadOnly)
		if ecode != 0 {
			err = fmt.Errorf("%v (%d)", err, ecode)
		}
		return err
	}

	if testClusterType == ClusterTypeK8s {
		// For kubernetes cluster, we use LoadBalancer service to expose the proxies.
		// `proxyURLReadOnly` will point to LoadBalancer service.
		return nil
	}

	// Primary proxy can change if proxy tests are run and
	// no new cluster is re-deployed before each test.
	// Find the current primary.
	primary, err := GetPrimaryProxy(proxyURLReadOnly)
	if err != nil {
		err = fmt.Errorf("failed to get primary proxy info from %s; err %v", proxyURLReadOnly, err)
		return err
	}
	proxyURLReadOnly = primary.URL(cmn.NetPublic)
	return nil
}

func initPmap() error {
	bp := BaseAPIParams(proxyURLReadOnly)
	smap, err := waitForStartup(bp)
	if err != nil {
		return err
	}
	pmapReadOnly = smap.Pmap
	return nil
}

func initRemAis() {
	all, err := api.GetRemoteAIS(BaseAPIParams(proxyURLReadOnly))
	if err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "failed to query remote ais cluster: %v\n", err)
		}
		return
	}
	cos.AssertMsg(len(all.A) < 2, "multi-remote clustering is not implemented yet")
	if len(all.A) == 1 {
		remais := all.A[0]
		RemoteCluster.UUID = remais.UUID
		RemoteCluster.Alias = remais.Alias
		RemoteCluster.URL = remais.URL
	}
}

func initNodeCmd() {
	bp := BaseAPIParams(proxyURLReadOnly)
	smap, err := waitForStartup(bp)
	cos.AssertNoErr(err)
	restoreNodes = make(map[string]RestoreCmd, smap.CountProxies()+smap.CountTargets())
	for _, node := range smap.Pmap {
		if node.ID() == MockDaemonID {
			continue
		}
		restoreNodes[node.ID()] = GetRestoreCmd(node)
	}

	for _, node := range smap.Tmap {
		if node.ID() == MockDaemonID {
			continue
		}
		restoreNodes[node.ID()] = GetRestoreCmd(node)
	}
}

// reads .env file and parses its contents
func parseEnvVariables(fpath string, delimiter ...string) map[string]string {
	m := map[string]string{}
	dlim := "="
	data, err := os.ReadFile(fpath)
	if err != nil {
		return nil
	}

	if len(delimiter) > 0 {
		dlim = delimiter[0]
	}

	paramList := strings.Split(string(data), "\n")
	for _, dat := range paramList {
		datum := strings.Split(dat, dlim)
		// key=val
		if len(datum) == 2 {
			key := strings.TrimSpace(datum[0])
			value := strings.TrimSpace(datum[1])
			m[key] = value
		}
	}
	return m
}
