// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
package commands

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/config"
	"github.com/NVIDIA/aistore/cmn"
)

const loadCfgErrFmt = "WARNING: Failed to read config file (%v). Using default config.\n"

func loadConfig() *config.Config {
	cfg, err := config.Load()
	if err != nil {
		// Use default config in case of error.
		cfg = config.Default()

		// If config file wasn't found, create one.
		// Otherwise, warn the user.
		if os.IsNotExist(err) {
			config.SaveDefault()
		} else {
			fmt.Fprintf(os.Stderr, loadCfgErrFmt, err)
		}
	}
	return cfg
}

func initAuthParams() {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}

	tokenPath := filepath.Join(home, credDir, credFile)
	cmn.LocalLoad(tokenPath, &loggedUserToken)
}

func initClusterParams() {
	initAuthParams()

	clusterURL = determineClusterURL(cfg)

	defaultHTTPClient = &http.Client{
		Timeout: cfg.Timeout.HTTPTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: cfg.Timeout.TCPTimeout,
			}).DialContext,
		},
	}

	defaultAPIParams = &api.BaseParams{
		Client: defaultHTTPClient,
		URL:    clusterURL,
		Token:  loggedUserToken.Token,
	}
}

func init() {
	cfg = loadConfig()
	initClusterParams()
}
