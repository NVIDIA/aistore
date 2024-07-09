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
		MaxShardSize  int64
		Ext           string
		ShardTemplate string
		Collapse      bool
	}
	Config struct {
		ClusterConfig
		IshardConfig
		SrcBck   cmn.Bck
		DstBck   cmn.Bck
		Progress bool
	}
)

const (
	defaultClusterIPv4 = "127.0.0.1"
	defaultProxyPort   = "8080"
)

var DefaultConfig = Config{
	ClusterConfig: ClusterConfig{URL: "http://" + defaultClusterIPv4 + ":" + defaultProxyPort},
	IshardConfig:  IshardConfig{MaxShardSize: 102400, Ext: ".tar", ShardTemplate: "shard-%d", Collapse: false},
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
	flag.StringVar(&cfg.ShardTemplate, "shard_template", "shard-%d", "the template used for generating output shards. Accepts Bash (prefix{0001..0010}suffix), Fmt (prefix-%06d-suffix), or At (prefix-@00001-gap-@100-suffix) templates")
	flag.StringVar(&cfg.Ext, "ext", ".tar", "the extension used for generating output shards.")
	flag.BoolVar(&cfg.Collapse, "collapse", false, "If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.")
	flag.BoolVar(&cfg.Progress, "progress", false, "If true, display the progress of processing objects in the source bucket.")
	flag.Parse()

	if cfg.SrcBck.Provider, cfg.SrcBck.Name = cmn.ParseURLScheme(cfg.SrcBck.Name); cfg.SrcBck.Provider == "" {
		cfg.SrcBck.Provider = apc.AIS
	}
	if cfg.DstBck.Provider, cfg.DstBck.Name = cmn.ParseURLScheme(cfg.DstBck.Name); cfg.DstBck.Provider == "" {
		cfg.DstBck.Provider = apc.AIS
	}
}
