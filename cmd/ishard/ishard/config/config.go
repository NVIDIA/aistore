// Package config provides types and functions to configure ishard executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort"
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
		SortFlag
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
	SortFlag:      SortFlag{IsSet: false},
}

////////////////////////
// Parse `-sort` flag //
////////////////////////

type SortFlag struct {
	dsort.Algorithm
	IsSet bool
}

func (alg *SortFlag) Set(value string) error {
	parts := strings.Split(value, ":")
	if len(parts) == 0 {
		return fmt.Errorf("invalid sort flag format")
	}

	alg.IsSet = true

	switch parts[0] {
	case "alpha", "alphanumeric":
		alg.Kind = "alphanumeric"
		if len(parts) > 1 {
			if parts[1] == "inc" {
				alg.Decreasing = false
			} else if parts[1] == "dec" {
				alg.Decreasing = true
			} else {
				return fmt.Errorf("invalid alphanumeric sort option, expected 'inc' or 'dec'")
			}
		}
	case "shuffle":
		alg.Kind = "shuffle"
		if len(parts) > 1 {
			alg.Seed = parts[1]
			if _, err := strconv.ParseInt(alg.Seed, 10, 64); err != nil {
				return fmt.Errorf("invalid shuffle seed, must be a valid integer")
			}
		}
	default:
		return fmt.Errorf("invalid sort kind, expected 'alpha' or 'shuffle'")
	}

	return nil
}

func (alg *SortFlag) String() string {
	switch alg.Kind {
	case "alpha", "alphanumeric":
		if alg.Decreasing {
			return "alpha:dec"
		}
		return "alpha:inc"
	case "shuffle":
		if alg.Seed != "" {
			return "shuffle:" + alg.Seed
		}
		return "shuffle"
	default:
		return ""
	}
}

///////////////////////////
// Parse `-dry_run` flag //
///////////////////////////

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
	flag.StringVar(&cfg.SrcBck.Name, "src_bck", "", "Source bucket name or URI.")
	flag.StringVar(&cfg.DstBck.Name, "dst_bck", "", "Destination bucket name or URI.")
	flag.StringVar(&cfg.ShardTemplate, "shard_template", "shard-%d", "Template used for generating output shards. Accepts Bash (prefix{0001..0010}suffix), Fmt (prefix-%06d-suffix), or At (prefix-@00001-gap-@100-suffix) templates")
	flag.StringVar(&cfg.Ext, "ext", ".tar", "Extension used for generating output shards.")
	flag.StringVar(&cfg.MissingExtAction, "missing_extension_action", "ignore", "Action to take when an extension is missing: abort | warn | ignore")
	flag.BoolVar(&cfg.Collapse, "collapse", false, "If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.")
	flag.BoolVar(&cfg.Progress, "progress", false, "If true, display the progress of processing objects in the source bucket.")
	flag.Var(&cfg.DryRunFlag, "dry_run", "If set, only shows the layout of resulting output shards without actually executing archive jobs. Use 'show_keys' to include sample keys.")
	flag.Var(&cfg.SortFlag, "sort", "sorting algorithm (e.g., alpha:inc, alpha:dec, shuffle, shuffle:seed)")

	var (
		err                 error
		maxShardSizeStr     string
		sampleExts          string
		sampleKeyPatternStr string
	)

	flag.StringVar(&maxShardSizeStr, "max_shard_size", "1MiB", "Maximum size of each output shard. Accepts IEC, SI, and raw formats.")
	flag.StringVar(&sampleExts, "sample_exts", "", "Comma-separated list of extensions that should exists in the dataset.")
	flag.StringVar(&sampleKeyPatternStr, "sample_key_pattern", "", "The regex pattern used to transform object names in the source bucket to sample keys. This ensures that objects with the same sample key are always sharded into the same output shard.")

	flag.Parse()

	if cfg.MaxShardSize, err = cos.ParseSize(maxShardSizeStr, cos.UnitsIEC); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(1)
	}

	if _, ok := MissingExtActMap[cfg.MissingExtAction]; !ok {
		fmt.Fprintf(os.Stderr, "Invalid action: %s. Accepted values are: abort, warn, ignore\n", cfg.MissingExtAction)
		flag.Usage()
		os.Exit(1)
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
		fmt.Println("`sample_key_pattern` is not specified, use `base_file_name` as sample key by default.")
		cfg.SampleKeyPattern = BaseFileNamePattern
	} else if pattern, ok := commonPatterns[sampleKeyPatternStr]; ok {
		cfg.SampleKeyPattern = pattern
	} else {
		fmt.Printf("`sample_key_pattern` %s is not built-in (`base_file_name` | `full_name` | `collapse_all_dir`), compiled as custom regex\n", sampleKeyPatternStr)
		if _, err := regexp.Compile(sampleKeyPatternStr); err != nil {
			fmt.Fprintln(os.Stderr, err)
			flag.Usage()
			os.Exit(1)
		}
		cfg.SampleKeyPattern = SampleKeyPattern{Regex: sampleKeyPatternStr, CaptureGroup: "$1"}
	}

	if cfg.SrcBck.Name == "" || cfg.DstBck.Name == "" {
		fmt.Fprintln(os.Stderr, "Error: src_bck and dst_bck are required parameters.")
		flag.Usage()
		os.Exit(1)
	}

	if cfg.SrcBck, cfg.SrcPrefix, err = cmn.ParseBckObjectURI(cfg.SrcBck.Name, cmn.ParseURIOpts{DefaultProvider: apc.AIS}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(1)
	}
	if cfg.DstBck, _, err = cmn.ParseBckObjectURI(cfg.DstBck.Name, cmn.ParseURIOpts{DefaultProvider: apc.AIS}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(1)
	}
}
