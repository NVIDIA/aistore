// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"fmt"
	"os"
	"path/filepath"
)

////////////////
// app config //
////////////////

// configDir := cmn.AppConfigPath(appName)

// LoadAppConfig loads app config.
func LoadAppConfig(configDir, configFileName string, v interface{}) (err error) {
	// Check if config file exists.
	configFilePath := filepath.Join(configDir, configFileName)
	if _, err = os.Stat(configFilePath); err != nil {
		return err
	}

	// Load config from file.
	if _, err = Load(configFilePath, v, Options{Indent: true}); err != nil {
		return fmt.Errorf("failed to load config file %q: %v", configFilePath, err)
	}
	return
}

// configDir := cmn.AppConfigPath(appName)

// SaveAppConfig writes app config.
func SaveAppConfig(configDir, configFileName string, v interface{}) (err error) {
	// Check if config dir exists; if not, create one with default config.
	configFilePath := filepath.Join(configDir, configFileName)
	return Save(configFilePath, v, Options{Indent: true}, nil /*sgl*/)
}
