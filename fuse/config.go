// Command-line mounting utility for aisfs.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fuse/fs"
)

const configDirName = fs.Name

var defaultConfig = Config{
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
		// Determines the size of chunks that we write with append. The only exception
		// when we write less is Flush (end-of-file).
		WriteBufSize: cmn.MiB,
	},
}

type (
	Config struct {
		Cluster ClusterConfig `json:"cluster"`
		Timeout TimeoutConfig `json:"timeout"`
		Log     LogConfig     `json:"log"`
		IO      IOConfig      `json:"io"`
	}

	ClusterConfig struct {
		URL string `json:"url"`
	}

	TimeoutConfig struct {
		TCPTimeoutStr  string        `json:"tcp_timeout"`
		TCPTimeout     time.Duration `json:"-"`
		HTTPTimeoutStr string        `json:"http_timeout"`
		HTTPTimeout    time.Duration `json:"-"`
	}

	LogConfig struct {
		ErrorLogFile string `json:"error_log_file"`
		DebugLogFile string `json:"debug_log_file"`
	}

	IOConfig struct {
		WriteBufSize int64 `json:"write_buf_size"`
	}
)

func (c *Config) validate() (err error) {
	if c.Timeout.TCPTimeout, err = time.ParseDuration(c.Timeout.TCPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.tcp_timeout format %q: %v", c.Timeout.TCPTimeoutStr, err)
	}
	if c.Timeout.HTTPTimeout, err = time.ParseDuration(c.Timeout.HTTPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.http_timeout format %q: %v", c.Timeout.HTTPTimeoutStr, err)
	}
	if c.IO.WriteBufSize < 0 {
		return fmt.Errorf("bad io.write_buf_size value: %d, expected value non-negative", c.IO.WriteBufSize)
	}
	return nil
}

func loadConfig(bucket string) (cfg *Config, err error) {
	cfg = &Config{}
	configFileName := bucket + "_mount.json"
	if err = cmn.LoadAppConfig(configDirName, configFileName, &cfg); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load config: %v", err)
		}

		cfg = &defaultConfig
		err = cmn.SaveAppConfig(configDirName, configFileName, cfg)
		if err != nil {
			err = fmt.Errorf("failed to generate config file: %v", err)
		}
		return
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return
}
