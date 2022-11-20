// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os/exec"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
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

	clusterURL = determineClusterURL(cfg)
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

func detectK8s() bool {
	cmd := exec.Command("which", "kubectl")
	err := cmd.Run()
	return err == nil
}

func initSupportedCksumFlags() (flags []cli.Flag) {
	checksums := cos.SupportedChecksums()
	flags = make([]cli.Flag, 0, len(checksums)-1)
	for _, cksum := range checksums {
		if cksum == cos.ChecksumNone {
			continue
		}
		flags = append(flags, cli.StringFlag{
			Name:  cksum,
			Usage: fmt.Sprintf("hex encoded string of the %s checksum", cksum),
		})
	}
	return
}
