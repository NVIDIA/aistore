// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
package commands

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cli/config"
)

func loadConfig() *config.Config {
	cfg, err := config.Load()
	if err != nil {
		// Use default config in case of error.
		cfg = config.Default()

		// If config file wasn't found, create one.
		if os.IsNotExist(err) {
			config.SaveDefault()
		} else {
			fmt.Fprintf(os.Stderr, "WARNING: Failed to read config file, using default config.\n")
		}
	}
	return cfg
}

func init() {
	cfg = loadConfig()
	clusterURL = determineClusterURL(cfg)
}
