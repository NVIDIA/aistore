// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/tools/docker"
)

var loggedUserToken string

func Init() (err error) {
	cfg, err = config.Load()
	if err != nil {
		return err
	}
	k8sDetected = detectK8s()
	initClusterParams()
	return nil
}

// private

func initClusterParams() {
	loggedUserToken = authn.LoadToken("")

	clusterURL = _clusterURL(cfg)
	defaultHTTPClient = cmn.NewClient(cmn.TransportArgs{
		DialTimeout: cfg.Timeout.TCPTimeout,
		Timeout:     cfg.Timeout.HTTPTimeout,
		UseHTTPS:    cos.IsHTTPS(clusterURL),
		SkipVerify:  cfg.Cluster.SkipVerifyCrt,
	})

	if authnURL := cliAuthnURL(cfg); authnURL != "" {
		authnHTTPClient = cmn.NewClient(cmn.TransportArgs{
			DialTimeout: cfg.Timeout.TCPTimeout,
			Timeout:     cfg.Timeout.HTTPTimeout,
			UseHTTPS:    cos.IsHTTPS(authnURL),
			SkipVerify:  cfg.Cluster.SkipVerifyCrt,
		})

		authParams = api.BaseParams{
			Client: authnHTTPClient,
			URL:    authnURL,
			Token:  loggedUserToken,
			UA:     ua,
		}
	}

	apiBP = api.BaseParams{
		Client: defaultHTTPClient,
		URL:    clusterURL,
		Token:  loggedUserToken,
		UA:     ua,
	}
}

// resolving order:
// 1. cfg.Cluster.URL; if empty:
// 2. Proxy docker container IP address; if not successful:
// 3. Docker default; if not present:
// 4. Default as cfg.Cluster.DefaultAISHost
func _clusterURL(cfg *config.Config) string {
	if envURL := os.Getenv(env.AIS.Endpoint); envURL != "" {
		return envURL
	}
	if cfg.Cluster.URL != "" {
		return cfg.Cluster.URL
	}

	if docker.IsRunning() {
		clustersIDs, err := docker.ClusterIDs()
		if err != nil {
			fmt.Fprintf(os.Stderr, dockerErrMsgFmt, err, cfg.Cluster.DefaultDockerHost)
			return cfg.Cluster.DefaultDockerHost
		}

		debug.Assert(len(clustersIDs) > 0, "There should be at least one cluster running, when docker running detected.")

		proxyGateway, err := docker.ClusterEndpoint(clustersIDs[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, dockerErrMsgFmt, err, cfg.Cluster.DefaultDockerHost)
			return cfg.Cluster.DefaultDockerHost
		}

		if len(clustersIDs) > 1 {
			fmt.Fprintf(os.Stderr, "Multiple docker clusters running. Connected to %d via %s.\n", clustersIDs[0], proxyGateway)
		}

		return "http://" + proxyGateway + ":8080"
	}

	return cfg.Cluster.DefaultAISHost
}

func detectK8s() bool {
	cmd := exec.Command("which", "kubectl")
	err := cmd.Run()
	return err == nil
}
