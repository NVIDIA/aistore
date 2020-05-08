// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
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

var (
	proxyURLReadOnly string // user-defined primary proxy URL - it is read-only variable and tests mustn't change it

	transportArgs = cmn.TransportArgs{
		Timeout:          600 * time.Second,
		IdleConnsPerHost: 100,
		UseHTTPProxyEnv:  true,
	}
	DefaultHTTPClient = &http.Client{}
	HTTPClient        *http.Client
	HTTPClientGetPut  *http.Client

	RemoteCluster struct {
		UUID  string
		Alias string
		URL   string
	}

	MMSA *memsys.MMSA
)

func init() {
	MMSA = memsys.DefaultPageMM()
	envURL := os.Getenv(cmn.AISURLEnvVar)
	transportArgs.UseHTTPS = cmn.IsHTTPS(envURL)
	transportArgs.SkipVerify = cmn.IsParseBool(os.Getenv(cmn.AISSkipVerifyEnvVar))
	HTTPClient = cmn.NewClient(transportArgs)

	transportArgs.WriteBufferSize, transportArgs.ReadBufferSize = 65536, 65536
	HTTPClientGetPut = cmn.NewClient(transportArgs)

	initProxyURL()

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

func initProxyURL() {
	envVars := cmn.ParseEnvVariables(dockerEnvFile)                    // Gets the fields from the .env file from which the docker was deployed
	primaryHostIP, port := envVars["PRIMARY_HOST_IP"], envVars["PORT"] // Host IP and port of primary cluster

	proxyURLReadOnly = proxyURL
	if containers.DockerRunning() && proxyURLReadOnly == proxyURL {
		proxyURLReadOnly = "http://" + primaryHostIP + ":" + port
	}

	// This is needed for testing on Kubernetes if we want to run 'make test-XXX'
	// Many of the other packages do not accept the 'url' flag
	if cliAISURL := os.Getenv(cmn.AISURLEnvVar); cliAISURL != "" {
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
		cmn.ExitInfof("Failed to get primary proxy, err = %v", err)
	}
	proxyURLReadOnly = primary.URL(cmn.NetworkPublic)
}
