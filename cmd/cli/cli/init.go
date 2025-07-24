// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
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

func Init(args []string) (err error) {
	cfg, err = config.Load(args, cmdReset)
	if err != nil {
		return err
	}
	// kubernetes
	k8sDetected = detectK8s()

	// auth
	token := os.Getenv(env.AisAuthToken)
	tokenFile := os.Getenv(env.AisAuthTokenFile)

	if token != "" && tokenFile != "" {
		fmt.Fprintf(os.Stderr, "Warning: both `%s` and `%s` are set, using `%s`\n", env.AisAuthToken, env.AisAuthTokenFile, env.AisAuthToken)
	}

	loggedUserToken, _ = authn.LoadToken("") // No error handling as token might not be needed

	// http clients: the main one and the auth, if enabled
	clusterURL = _clusterURL(cfg)

	var (
		cargs = cmn.TransportArgs{
			DialTimeout: cfg.Timeout.TCPTimeout,
			Timeout:     cfg.Timeout.HTTPTimeout,
		}
		sargs = cmn.TLSArgs{
			ClientCA:    cfg.Cluster.ClientCA,
			Certificate: cfg.Cluster.Certificate,
			Key:         cfg.Cluster.CertKey,
			SkipVerify:  cfg.Cluster.SkipVerifyCrt,
		}
	)

	cmn.EnvToTLS(&sargs)

	apiBP = api.BaseParams{
		URL:   clusterURL,
		Token: loggedUserToken,
		UA:    ua,
	}
	if cos.IsHTTPS(clusterURL) {
		// TODO -- FIXME: cfg.WarnTLS("aistore at " + clusterURL)
		clientTLS = cmn.NewClientTLS(cargs, sargs, false /*intra-cluster*/)
		apiBP.Client = clientTLS
	} else {
		clientH = cmn.NewClient(cargs)
		apiBP.Client = clientH
	}

	if authnURL := cliAuthnURL(cfg); authnURL != "" {
		authParams = api.BaseParams{
			URL:   authnURL,
			Token: loggedUserToken,
			UA:    ua,
		}
		if cos.IsHTTPS(authnURL) {
			if clientTLS == nil {
				// TODO -- FIXME: cfg.WarnTLS("AuthN at " + authnURL)
				clientTLS = cmn.NewClientTLS(cargs, sargs, false /*intra-cluster*/)
			}
			authParams.Client = clientTLS
		} else {
			if clientH == nil {
				clientH = cmn.NewClient(cargs)
			}
			authParams.Client = clientH
		}
	}
	return nil
}

// resolving order:
// 1. cfg.Cluster.URL; if empty:
// 2. Proxy docker container IP address; if not successful:
// 3. Docker default; if not present:
// 4. Default as cfg.Cluster.DefaultAISHost
func _clusterURL(cfg *config.Config) string {
	if envURL := os.Getenv(env.AisEndpoint); envURL != "" {
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
	ctx, cancel := context.WithTimeout(context.Background(), execLinuxCommandTime)
	defer cancel()
	cmd := exec.CommandContext(ctx, "which", "kubectl")
	err := cmd.Run()
	return err == nil
}
