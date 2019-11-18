// Command-line mounting utility for aisfs.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fuse/fs"
)

const (
	configHomeEnvVar                 = "XDG_CONFIG_HOME"
	configDirName                    = fs.Name
	configFileNameSuffix             = "_mount.json"
	configDirMode        os.FileMode = 0755 | os.ModeDir
)

type Config struct {
	Cluster ClusterConfig `json:"cluster"`
	Timeout TimeoutConfig `json:"timeout"`
	Log     LogConfig     `json:"log"`
	IO      IOConfig      `json:"io"`
}

type ClusterConfig struct {
	URL string `json:"url"`
}

type TimeoutConfig struct {
	TCPTimeoutStr  string        `json:"tcp_timeout"`
	TCPTimeout     time.Duration `json:"-"`
	HTTPTimeoutStr string        `json:"http_timeout"`
	HTTPTimeout    time.Duration `json:"-"`
}

type LogConfig struct {
	ErrorLogFile string `json:"error_log_file"`
	DebugLogFile string `json:"debug_log_file"`
}

type IOConfig struct {
	WriteBufSize int64 `json:"write_buf_size"`
}

var defaultConfig Config

func init() {
	defaultConfig = Config{
		Cluster: ClusterConfig{
			URL: "http://127.0.0.1:8080",
		},
		Timeout: TimeoutConfig{
			TCPTimeoutStr:  "60s",
			TCPTimeout:     60 * time.Second,
			HTTPTimeoutStr: "300s",
			HTTPTimeout:    300 * time.Second,
		},
		Log: LogConfig{
			ErrorLogFile: "",
			DebugLogFile: "",
		},
		IO: IOConfig{
			WriteBufSize: maxWriteBufSize,
		},
	}
}

func (c *Config) validate() (err error) {
	if c.Timeout.TCPTimeout, err = time.ParseDuration(c.Timeout.TCPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.tcp_timeout format %q: %v", c.Timeout.TCPTimeoutStr, err)
	}
	if c.Timeout.HTTPTimeout, err = time.ParseDuration(c.Timeout.HTTPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.http_timeout format %q: %v", c.Timeout.HTTPTimeoutStr, err)
	}
	// TODO: Validate IOConfig
	return nil
}

func tryLoadConfig(bucket string) (cfg *Config, err error) {
	var configDir string

	// Determine the location of config directory
	if cfgHome := os.Getenv(configHomeEnvVar); cfgHome != "" {
		// $XDG_CONFIG_HOME/aisfs
		configDir = filepath.Join(cfgHome, configDirName)
	} else {
		var home string
		home, err = homeDir()
		if err != nil {
			return
		}
		configDir = filepath.Join(home, ".config", configDirName)
	}

	if cfg, err = loadConfig(configDir, bucket); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		cfg = &defaultConfig
		err = nil

		// Ignore errors.
		saveDefaultConfig(configDir, bucket)
	}
	return
}

// loadConfig reads a config object from the config file.
// Bucket name is passed to this function because each
// bucket mount can have its own config file.
func loadConfig(configDir string, bucket string) (cfg *Config, err error) {
	// Determine the path to config file.
	configFileName := bucket + configFileNameSuffix
	configFilePath := filepath.Join(configDir, configFileName)

	// Check if config file exists.
	if _, err = os.Stat(configFilePath); err != nil {
		return nil, err
	}

	// Load config from file.
	cfg = &Config{}
	err = cmn.LocalLoad(configFilePath, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load config file %q: %v", configFilePath, err)
	}

	err = cfg.validate()
	if err != nil {
		return nil, fmt.Errorf("failed to load config file %q: %v", configFilePath, err)
	}

	return
}

// saveDefault writes the default config object to the config file.
func saveDefaultConfig(configDir string, bucket string) (err error) {
	// Determine the path to config file.
	configFileName := bucket + configFileNameSuffix
	configFilePath := filepath.Join(configDir, configFileName)

	// Check if config dir exists; if not, create one with default config.
	if _, err = os.Stat(configDir); os.IsNotExist(err) {
		err = os.MkdirAll(configDir, configDirMode)
		if err != nil {
			return
		}
		return cmn.LocalSave(configFilePath, &defaultConfig)
	}

	// It is some error other than NotExist.

	if err != nil {
		return
	}

	// Check if config file exists; if not, create one with default config.
	if _, err = os.Stat(configFilePath); os.IsNotExist(err) {
		return cmn.LocalSave(configFilePath, &defaultConfig)
	}

	return
}
