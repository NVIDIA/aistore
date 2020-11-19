// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	registerTimeout = time.Minute * 2
	bucketTimeout   = time.Minute
)

const (
	proxyURL      = "http://localhost:8080"      // the url for the cluster's proxy (local)
	dockerEnvFile = "/tmp/docker_ais/deploy.env" // filepath of Docker deployment config
)

type (
	// command used to restore a node
	RestoreCmd struct {
		Node *cluster.Snode
		Cmd  string
		Args []string
	}
)

var (
	proxyURLReadOnly string          // user-defined primary proxy URL - it is read-only variable and tests mustn't change it
	pmapReadOnly     cluster.NodeMap // initial proxy map - it is read-only variable

	restoreNodesOnce sync.Once             // Ensures that the initialization happens only once.
	restoreNodes     map[string]RestoreCmd // initial proxy and target nodes => command to restore them

	transportArgs = cmn.TransportArgs{
		Timeout:          600 * time.Second,
		IdleConnsPerHost: 100,
		UseHTTPProxyEnv:  true,
	}
	HTTPClient *http.Client

	RemoteCluster struct {
		UUID  string
		Alias string
		URL   string
	}
	AuthToken string

	MMSA *memsys.MMSA

	devtoolsCtx *devtools.Ctx
)

func init() {
	MMSA = memsys.DefaultPageMM()
	envURL := os.Getenv(cmn.EnvVars.Endpoint)

	// Since tests do not have access to cluster configuration, the tests
	// detect client type by the primary proxy URL passed by a user.
	// Certificate check is always disabled.
	transportArgs.UseHTTPS = cmn.IsHTTPS(envURL)
	transportArgs.SkipVerify = cmn.IsParseBool(os.Getenv(cmn.EnvVars.SkipVerifyCrt))
	HTTPClient = cmn.NewClient(transportArgs)

	devtoolsCtx = &devtools.Ctx{
		Client: HTTPClient,
		Log:    Logf,
	}

	initProxyURL()
	initPmap()
	initRemoteCluster()
	initAuthToken()
}

func initProxyURL() {
	// Gets the fields from the .env file from which the docker was deployed
	envVars := cmn.ParseEnvVariables(dockerEnvFile)
	// Host IP and port of primary cluster
	primaryHostIP, port := envVars["PRIMARY_HOST_IP"], envVars["PORT"]

	proxyURLReadOnly = proxyURL
	if containers.DockerRunning() {
		proxyURLReadOnly = "http://" + primaryHostIP + ":" + port
	}

	// This is needed for testing on Kubernetes if we want to run 'make test-XXX'
	// Many of the other packages do not accept the 'url' flag
	if cliAISURL := os.Getenv(cmn.EnvVars.Endpoint); cliAISURL != "" {
		if !strings.HasPrefix(cliAISURL, "http") {
			cliAISURL = "http://" + cliAISURL
		}
		proxyURLReadOnly = cliAISURL
	}

	// Primary proxy can change if proxy tests are run and
	// no new cluster is re-deployed before each test.
	// Finds who is the current primary proxy.
	primary, err := GetPrimaryProxy(proxyURLReadOnly)
	if err != nil {
		fmt.Printf("Failed to reach primary proxy at %s\n", proxyURLReadOnly)
		fmt.Printf("Error: %s\n", strings.TrimSuffix(err.Error(), "\n"))
		fmt.Println("Environment variables:")
		fmt.Printf("\t%s:\t%s\n", cmn.EnvVars.Endpoint, os.Getenv(cmn.EnvVars.Endpoint))
		fmt.Printf("\t%s:\t%s\n", cmn.EnvVars.PrimaryID, os.Getenv(cmn.EnvVars.PrimaryID))
		fmt.Printf("\t%s:\t%s\n", cmn.EnvVars.SkipVerifyCrt, os.Getenv(cmn.EnvVars.SkipVerifyCrt))
		fmt.Printf("\t%s:\t%s\n", cmn.EnvVars.UseHTTPS, os.Getenv(cmn.EnvVars.UseHTTPS))
		if len(envVars) > 0 {
			fmt.Println("Docker Environment:")
			for k, v := range envVars {
				fmt.Printf("\t%s:\t%s\n", k, v)
			}
		}
		cmn.Exitf("")
	}
	proxyURLReadOnly = primary.URL(cmn.NetworkPublic)
}

func initPmap() {
	baseParams := BaseAPIParams(proxyURLReadOnly)
	smap, err := waitForStartup(baseParams)
	cmn.AssertNoErr(err)
	pmapReadOnly = smap.Pmap
}

func initRemoteCluster() {
	aisInfo, err := api.GetRemoteAIS(BaseAPIParams(proxyURLReadOnly))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get remote cluster information: %v", err)
	} else {
		for _, clusterInfo := range aisInfo {
			if !clusterInfo.Online {
				continue
			}
			// TODO: use actual UUID (for now it doesn't work correctly as
			//  proxy may not have full information about the remote cluster)
			RemoteCluster.UUID = clusterInfo.Alias
			RemoteCluster.Alias = clusterInfo.Alias
			RemoteCluster.URL = clusterInfo.URL
			break
		}
	}
}

func initNodeCmd() {
	baseParams := BaseAPIParams(proxyURLReadOnly)
	smap, err := waitForStartup(baseParams)
	cmn.AssertNoErr(err)
	restoreNodes = make(map[string]RestoreCmd, smap.CountProxies()+smap.CountTargets())
	for _, node := range smap.Pmap {
		restoreNodes[node.ID()] = getRestoreCmd(node)
	}

	for _, node := range smap.Tmap {
		restoreNodes[node.ID()] = getRestoreCmd(node)
	}
}

func initAuthToken() {
	home, err := os.UserHomeDir()
	cmn.AssertNoErr(err)
	tokenPath := filepath.Join(home, ".ais", "token")

	var token api.AuthCreds
	jsp.Load(tokenPath, &token, jsp.Plain())

	AuthToken = token.Token
}
