// Package config provides types and functions to configure AIS CLI.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	configHomeEnvVar             = "XDG_CONFIG_HOME"
	configDirName                = "ais"
	configFileName               = "config.json"
	configDirMode    os.FileMode = 0755 | os.ModeDir
)

type Config struct {
	Cluster ClusterConfig `json:"cluster"`
}

type ClusterConfig struct {
	URL               string `json:"url"`
	DefaultAISHost    string `json:"default_ais_host"`
	K8SNamespace      string `json:"k8s_namespace"`
	DefaultDockerHost string `json:"default_docker_host"`
}

var (
	defaultConfig = Config{
		Cluster: ClusterConfig{
			URL:               "",
			DefaultAISHost:    "http://127.0.0.1:8080",
			K8SNamespace:      "",
			DefaultDockerHost: "http://172.50.0.2:8080",
		},
	}

	configDirPath  string
	configFilePath string
)

func init() {
	if dir := os.Getenv(configHomeEnvVar); dir != "" {
		configDirPath = filepath.Join(dir, configDirName)
	} else {
		configDirPath = filepath.Join(os.Getenv("HOME"), ".config", configDirName)
	}
	configFilePath = filepath.Join(configDirPath, configFileName)
}

func saveDefault() error {
	return cmn.LocalSave(configFilePath, &defaultConfig)
}

func createDirAndSaveDefault() (err error) {
	err = os.MkdirAll(configDirPath, configDirMode)
	if err != nil {
		return
	}
	return saveDefault()
}

// Default returns the default config object.
func Default() *Config {
	return &defaultConfig
}

// Load reads a config object from the config file.
func Load() (cfg *Config, err error) {
	// First, check if config file exists.
	if _, err = os.Stat(configFilePath); err != nil {
		return
	}

	// Load config from file.
	cfg = &Config{}
	err = cmn.LocalLoad(configFilePath, &cfg)
	if err != nil {
		return nil, err
	}

	return
}

// SaveDefault writes the default config object to the config file.
func SaveDefault() (err error) {
	// Check if config dir exists; if not, create one with default config.
	if _, err = os.Stat(configDirPath); os.IsNotExist(err) {
		return createDirAndSaveDefault()
	}

	// It is some error other than NotExist.
	if err != nil {
		return
	}

	// Check if config file exists; if not, create one with default config.
	if _, err = os.Stat(configFilePath); os.IsNotExist(err) {
		return saveDefault()
	}

	return
}
