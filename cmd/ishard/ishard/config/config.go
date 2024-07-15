// Package config provides types and functions to configure ishard executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"flag"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	ClusterConfig struct {
		URL string
	}
	IshardConfig struct {
		MaxShardSize     int64
		Ext              string
		ShardTemplate    string
		SampleKeyPattern SampleKeyPattern
		SampleExtensions []string
		MissingExtAction string
		Collapse         bool
	}
	DryRunFlag struct {
		IsSet bool
		Mode  string
	}
	Config struct {
		ClusterConfig
		IshardConfig
		DryRunFlag
		SrcBck    cmn.Bck
		SrcPrefix string
		DstBck    cmn.Bck
		Progress  bool
	}
)

const (
	defaultClusterIPv4 = "127.0.0.1"
	defaultProxyPort   = "8080"
)

var DefaultConfig = Config{
	ClusterConfig: ClusterConfig{URL: "http://" + defaultClusterIPv4 + ":" + defaultProxyPort},
	IshardConfig:  IshardConfig{MaxShardSize: 102400, Ext: ".tar", ShardTemplate: "shard-%d", Collapse: false, SampleKeyPattern: BaseFileNamePattern, MissingExtAction: "ignore"},
	SrcBck:        cmn.Bck{Name: "src_bck", Provider: apc.AIS},
	DstBck:        cmn.Bck{Name: "dst_bck", Provider: apc.AIS},
	Progress:      false,
	DryRunFlag:    DryRunFlag{IsSet: false},
}

func (d *DryRunFlag) String() string {
	return d.Mode
}

func (d *DryRunFlag) Set(value string) error {
	if value == "true" || value == "false" {
		d.IsSet = value == "true"
		d.Mode = ""
	} else {
		d.IsSet = true
		d.Mode = value
	}
	return nil
}

func (d *DryRunFlag) IsBoolFlag() bool {
	return true
}

// Load configuration for ishard from cli, or spec files (TODO)
func Load() (*Config, error) {
	cfg := DefaultConfig
	parseCliParams(&cfg)
	return &cfg, nil
}

func parseCliParams(cfg *Config) {
	flag.Int64Var(&cfg.MaxShardSize, "max_shard_size", 1024000, "Desired size of each output shard")
	flag.StringVar(&cfg.SrcBck.Name, "src_bck", "", "Source bucket name or URI.")
	flag.StringVar(&cfg.DstBck.Name, "dst_bck", "", "Destination bucket name or URI.")
	flag.StringVar(&cfg.ShardTemplate, "shard_template", "shard-%d", "Template used for generating output shards. Accepts Bash (prefix{0001..0010}suffix), Fmt (prefix-%06d-suffix), or At (prefix-@00001-gap-@100-suffix) templates")
	flag.StringVar(&cfg.Ext, "ext", ".tar", "Extension used for generating output shards.")
	flag.StringVar(&cfg.MissingExtAction, "missing_extension_action", "ignore", "Action to take when an extension is missing: abort | warn | ignore")
	flag.BoolVar(&cfg.Collapse, "collapse", false, "If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.")
	flag.BoolVar(&cfg.Progress, "progress", false, "If true, display the progress of processing objects in the source bucket.")
	flag.Var(&cfg.DryRunFlag, "dry_run", "If set, only shows the layout of resulting output shards without actually executing archive jobs. Use 'show_keys' to include sample keys.")

	var (
		sampleExts          string
		sampleKeyPatternStr string
	)

	flag.StringVar(&sampleExts, "sample_exts", "", "Comma-separated list of extensions that should exists in the dataset.")
	flag.StringVar(&sampleKeyPatternStr, "sample_key_pattern", "", "The regex pattern used to transform object names in the source bucket to sample keys. This ensures that objects with the same sample key are always sharded into the same output shard.")

	flag.Parse()

	if _, ok := MissingExtActMap[cfg.MissingExtAction]; !ok {
		log.Fatalf("Invalid action: %s. Accepted values are: abort, warn, ignore\n", cfg.MissingExtAction)
	}
	if sampleExts != "" {
		cfg.SampleExtensions = strings.Split(sampleExts, ",")
	}

	var commonPatterns = map[string]SampleKeyPattern{
		"base_file_name":   BaseFileNamePattern,
		"full_name":        FullNamePattern,
		"collapse_all_dir": CollapseAllDirPattern,
	}

	if sampleKeyPatternStr == "" {
		log.Printf("`sample_key_pattern` is not specified, use `base_file_name` as sample key by default.")
		cfg.SampleKeyPattern = BaseFileNamePattern
	} else if pattern, ok := commonPatterns[sampleKeyPatternStr]; ok {
		cfg.SampleKeyPattern = pattern
	} else {
		log.Printf("`sample_key_pattern` %s is not built-in (`base_file_name` | `full_name` | `collapse_all_dir`), compiled as custom regex.", sampleKeyPatternStr)
		if _, err := regexp.Compile(sampleKeyPatternStr); err != nil {
			log.Printf("Invalid regex pattern: %s. Error: %v", cfg.SampleKeyPattern, err)
			flag.Usage()
			os.Exit(1)
		}
		cfg.SampleKeyPattern = SampleKeyPattern{Regex: sampleKeyPatternStr, CaptureGroup: "$1"}
	}

	if cfg.SrcBck.Name == "" || cfg.DstBck.Name == "" {
		log.Println("Error: src_bck and dst_bck are required parameters.")
		flag.Usage()
		os.Exit(1)
	}

	var err error
	if cfg.SrcBck, cfg.SrcPrefix, err = cmn.ParseBckObjectURI(cfg.SrcBck.Name, cmn.ParseURIOpts{DefaultProvider: apc.AIS}); err != nil {
		log.Printf("Error on parsing source bucket: %s. Error: %v", cfg.SrcBck.Name, err)
		flag.Usage()
		os.Exit(1)
	}
	if cfg.DstBck, _, err = cmn.ParseBckObjectURI(cfg.DstBck.Name, cmn.ParseURIOpts{DefaultProvider: apc.AIS}); err != nil {
		log.Printf("Error on parsing destination bucket: %s. Error: %v", cfg.SrcBck.Name, err)
		flag.Usage()
		os.Exit(1)
	}
}
