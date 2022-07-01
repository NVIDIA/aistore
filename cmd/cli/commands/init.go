// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os/exec"
	"regexp"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

var loggedUserToken string

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
		}
	}

	defaultAPIParams = api.BaseParams{
		Client: defaultHTTPClient,
		URL:    clusterURL,
		Token:  loggedUserToken,
	}
}

func detectK8s() bool {
	cmd := exec.Command("which", "kubectl")
	err := cmd.Run()
	return err == nil
}

func Init() (err error) {
	unreachableRegex = regexp.MustCompile("dial.*(timeout|refused)")
	cfg, err = config.Load()
	if err != nil {
		return err
	}
	k8sDetected = detectK8s()
	initClusterParams()
	return nil
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
