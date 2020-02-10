// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

func initAuthParams() {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}

	// TODO: `credDir` should be `home/.config/ais/`
	tokenPath := filepath.Join(home, credDir, credFile)
	jsp.Load(tokenPath, &loggedUserToken, jsp.Plain())
}

func initClusterParams() {
	initAuthParams()

	clusterURL = determineClusterURL(cfg)

	defaultHTTPClient = cmn.NewClient(cmn.TransportArgs{
		DialTimeout: cfg.Timeout.TCPTimeout,
		Timeout:     cfg.Timeout.HTTPTimeout,

		IdleConnsPerHost: 100,
		MaxIdleConns:     100,
	})

	defaultAPIParams = api.BaseParams{
		Client: defaultHTTPClient,
		URL:    clusterURL,
		Token:  loggedUserToken.Token,
	}
}

func Init() (err error) {
	cfg, err = config.Load()
	initClusterParams()
	return err
}
