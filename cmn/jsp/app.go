// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
)

////////////////
// app config //
////////////////

// LoadAppConfig reads a config object from the config file.
func LoadAppConfig(appName, configFileName string, v interface{}) (err error) {
	// Check if config file exists.
	configDir := cmn.AppConfigPath(appName)
	configFilePath := filepath.Join(configDir, configFileName)
	if _, err = os.Stat(configFilePath); err != nil {
		return err
	}

	// Load config from file.
	if err = Load(configFilePath, v, Options{Indent: true}); err != nil {
		return fmt.Errorf("failed to load config file %q: %v", configFilePath, err)
	}
	return
}

// SaveAppConfig writes the config object to the config file.
func SaveAppConfig(appName, configFileName string, v interface{}) (err error) {
	// Check if config dir exists; if not, create one with default config.
	configDir := cmn.AppConfigPath(appName)
	configFilePath := filepath.Join(configDir, configFileName)
	return Save(configFilePath, v, Options{Indent: true})
}
