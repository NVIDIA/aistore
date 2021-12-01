// Package aisfs - command-line mounting utility for aisfs.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/cmd/aisfs/fs"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const configDirName = fs.Name

type (
	Config struct {
		Cluster     ClusterConfig  `json:"cluster"`
		Timeout     TimeoutConfig  `json:"timeout"`
		Periodic    PeriodicConfig `json:"periodic"`
		Log         LogConfig      `json:"log"`
		IO          IOConfig       `json:"io"`
		MemoryLimit string         `json:"memory_limit"`
	}
	ClusterConfig struct {
		URL           string `json:"url"`
		SkipVerifyCrt bool   `json:"skip_verify_crt"`
	}
	TimeoutConfig struct {
		TCPTimeoutStr  string        `json:"tcp_timeout"`
		TCPTimeout     time.Duration `json:"-"`
		HTTPTimeoutStr string        `json:"http_timeout"`
		HTTPTimeout    time.Duration `json:"-"`
	}
	PeriodicConfig struct {
		SyncIntervalStr string        `json:"sync_interval"`
		SyncInterval    time.Duration `json:"-"`
	}
	LogConfig struct {
		ErrorFile string `json:"error_file"`
		DebugFile string `json:"debug_file"`
	}
	IOConfig struct {
		WriteBufSize int64 `json:"write_buf_size"`
	}
)

var defaultConfig = Config{
	Cluster: ClusterConfig{
		URL:           "http://127.0.0.1:8080",
		SkipVerifyCrt: cos.IsParseBool(os.Getenv(cmn.EnvVars.SkipVerifyCrt)),
	},
	Timeout: TimeoutConfig{
		TCPTimeoutStr:  "60s",
		TCPTimeout:     60 * time.Second,
		HTTPTimeoutStr: "0s",
		HTTPTimeout:    0,
	},
	Periodic: PeriodicConfig{
		SyncIntervalStr: "20m",
		SyncInterval:    20 * time.Minute,
	},
	Log: LogConfig{
		ErrorFile: "",
		DebugFile: "",
	},
	IO: IOConfig{
		// Determines the size of chunks that we write with append. The only exception
		// when we write less is Flush (end-of-file).
		WriteBufSize: cos.MiB,
	},
	// By default we allow unlimited memory to be used by the cache.
	MemoryLimit: "0B",
}

func (c *Config) validate() (err error) {
	if c.Timeout.TCPTimeout, err = time.ParseDuration(c.Timeout.TCPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.tcp_timeout format %q: %v", c.Timeout.TCPTimeoutStr, err)
	}
	if c.Timeout.HTTPTimeout, err = time.ParseDuration(c.Timeout.HTTPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.http_timeout format %q: %v", c.Timeout.HTTPTimeoutStr, err)
	}
	if c.Periodic.SyncInterval, err = time.ParseDuration(c.Periodic.SyncIntervalStr); err != nil {
		return fmt.Errorf("invalid periodic.sync_interval format %q: %v", c.Periodic.SyncInterval, err)
	}
	if c.Log.ErrorFile != "" && !filepath.IsAbs(c.Log.ErrorFile) {
		return fmt.Errorf("invalid log.error_log_file format %q: path needs to be absolute", c.Log.ErrorFile)
	}
	if c.Log.DebugFile != "" && !filepath.IsAbs(c.Log.DebugFile) {
		return fmt.Errorf("invalid log.debug_log_file format %q: path needs to be absolute", c.Log.DebugFile)
	}
	if c.IO.WriteBufSize < 0 {
		return fmt.Errorf("invalid io.write_buf_size value: %d: expected non-negative value", c.IO.WriteBufSize)
	}
	if v, err := cos.S2B(c.MemoryLimit); err != nil {
		return fmt.Errorf("invalid memory_limit value: %q: %v", c.MemoryLimit, err)
	} else if v < 0 {
		return fmt.Errorf("invalid memory_limit value: %q: expected non-negative value", c.MemoryLimit)
	}
	return nil
}

func (c *Config) writeTo(srvCfg *fs.ServerConfig) {
	memoryLimit, _ := cos.S2B(c.MemoryLimit)
	srvCfg.SkipVerifyCrt = c.Cluster.SkipVerifyCrt
	srvCfg.TCPTimeout = c.Timeout.TCPTimeout
	srvCfg.HTTPTimeout = c.Timeout.HTTPTimeout
	srvCfg.SyncInterval.Store(c.Periodic.SyncInterval)
	srvCfg.MemoryLimit.Store(uint64(memoryLimit))
	srvCfg.MaxWriteBufSize.Store(c.IO.WriteBufSize)
}

func loadConfig(bucket string) (cfg *Config, err error) {
	var (
		configFileName = bucket + "_mount.json"
		configDirPath  = cmn.AppConfigPath(configDirName)
	)
	cfg = &Config{}
	if err = jsp.LoadAppConfig(configDirPath, configFileName, &cfg); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load config: %v", err)
		}

		cfg = &defaultConfig
		err = jsp.SaveAppConfig(configDirPath, configFileName, cfg)
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
