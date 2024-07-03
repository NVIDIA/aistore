// Package config provides types and functions to configure ishard executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"flag"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	ClusterConfig struct {
		URL string
	}
	IshardConfig struct {
		MaxShardSize int64
		StartIdx     int
		IdxDigits    int
		Ext          string
		Prefix       string
		Collapse     bool
	}
	Config struct {
		ClusterConfig
		IshardConfig
		SrcBck cmn.Bck
		DstBck cmn.Bck
	}
)

const (
	defaultClusterIPv4 = "127.0.0.1"
	defaultProxyPort   = "8080"
)

var DefaultConfig = Config{
	ClusterConfig: ClusterConfig{URL: "http://" + defaultClusterIPv4 + ":" + defaultProxyPort},
	IshardConfig:  IshardConfig{MaxShardSize: 102400, StartIdx: 0, IdxDigits: 4, Ext: ".tar", Prefix: "shard-", Collapse: false},
	SrcBck:        cmn.Bck{Name: "src_bck", Provider: apc.AIS},
	DstBck:        cmn.Bck{Name: "dst_bck", Provider: apc.AIS},
}

// Load configuration for ishard from cli, or spec files (TODO)
func Load() (*Config, error) {
	cfg := DefaultConfig
	parseCliParams(&cfg)
	return &cfg, nil
}

func parseCliParams(cfg *Config) {
	flag.Int64Var(&cfg.MaxShardSize, "max_shard_size", 1024000, "desired size of each output shard")
	flag.StringVar(&cfg.SrcBck.Name, "src_bck", "", "the source bucket name or URI. If empty, a bucket with random name will be created")
	flag.StringVar(&cfg.DstBck.Name, "dst_bck", "", "the destination bucket name or URI. If empty, a bucket with random name will be created")
	flag.BoolVar(&cfg.Collapse, "collapse", false, "If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.")
	flag.Parse()

	if cfg.SrcBck.Provider, cfg.SrcBck.Name = cmn.ParseURLScheme(cfg.SrcBck.Name); cfg.SrcBck.Provider == "" {
		cfg.SrcBck.Provider = apc.AIS
	}
	if cfg.DstBck.Provider, cfg.DstBck.Name = cmn.ParseURLScheme(cfg.DstBck.Name); cfg.DstBck.Provider == "" {
		cfg.DstBck.Provider = apc.AIS
	}
}
