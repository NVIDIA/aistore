// Package config provides types and functions to configure AIS CLI.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	configDirName  = "ais"
	configFileName = "config.json"
)

var (
	configFilePath string
	defaultConfig  = Config{
		Cluster: ClusterConfig{
			URL:               "http://127.0.0.1:8080",
			DefaultAISHost:    "http://127.0.0.1:8080",
			DefaultDockerHost: "http://172.50.0.2:8080",
		},
		Timeout: TimeoutConfig{
			TCPTimeoutStr:  "60s",
			TCPTimeout:     60 * time.Second,
			HTTPTimeoutStr: "300s",
			HTTPTimeout:    300 * time.Second,
		},
	}
)

func init() {
	configDirPath := cmn.AppConfigPath(configDirName)
	configFilePath = filepath.Join(configDirPath, configFileName)
}

type Config struct {
	Cluster ClusterConfig `json:"cluster"`
	Timeout TimeoutConfig `json:"timeout"`
}

type ClusterConfig struct {
	URL               string `json:"url"`
	DefaultAISHost    string `json:"default_ais_host"`
	DefaultDockerHost string `json:"default_docker_host"`
}

type TimeoutConfig struct {
	TCPTimeoutStr  string        `json:"tcp_timeout"`
	TCPTimeout     time.Duration `json:"-"`
	HTTPTimeoutStr string        `json:"http_timeout"`
	HTTPTimeout    time.Duration `json:"-"`
}

func (c *Config) validate() (err error) {
	if c.Timeout.TCPTimeout, err = time.ParseDuration(c.Timeout.TCPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.tcp_timeout format %q: %v", c.Timeout.TCPTimeoutStr, err)
	}
	if c.Timeout.HTTPTimeout, err = time.ParseDuration(c.Timeout.HTTPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.http_timeout format %q: %v", c.Timeout.HTTPTimeoutStr, err)
	}
	return nil
}

// Location returns an absolute path to the config file.
func Location() string {
	return configFilePath
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := cmn.LoadAppConfig(configDirName, configFileName, &cfg); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load config: %v", err)
		}

		// Use default config in case of error.
		cfg = &defaultConfig
		err = cmn.SaveAppConfig(configDirName, configFileName, cfg)
		if err != nil {
			err = fmt.Errorf("failed to generate config file: %v", err)
		}
		return cfg, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
