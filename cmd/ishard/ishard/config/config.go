// Package config provides types and functions to configure ishard executable.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"errors"
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
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	jsoniter "github.com/json-iterator/go"
)

type (
	ClusterConfig struct {
		URL string
	}
	IshardConfig struct {
		ShardSize        ShardSize
		Ext              string
		ShardTemplate    string
		SampleKeyPattern SampleKeyPattern
		MExtMgr          *MissingExtManager
		Collapse         bool
	}
	Config struct {
		ClusterConfig
		IshardConfig
		DryRunFlag
		SortFlag
		EKMFlag
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
	IshardConfig: IshardConfig{
		ShardSize:        ShardSize{Size: 102400},
		Ext:              ".tar",
		ShardTemplate:    "shard-%d",
		Collapse:         false,
		SampleKeyPattern: BaseFileNamePattern,
		MExtMgr:          nil,
	},
	SrcBck:     cmn.Bck{Name: "src_bck", Provider: apc.AIS},
	DstBck:     cmn.Bck{Name: "dst_bck", Provider: apc.AIS},
	Progress:   false,
	DryRunFlag: DryRunFlag{IsSet: false},
	SortFlag:   SortFlag{IsSet: false},
}

//////////////////////////////
// Parse `-shard_size` flag //
//////////////////////////////

type ShardSize struct {
	IsCount bool
	Size    int64
	Count   int
}

func (s *ShardSize) Set(value string) error {
	var (
		err error
		cnt int64
	)
	if cnt, err = strconv.ParseInt(value, 10, 32); err == nil {
		s.Count = int(cnt)
		s.IsCount = true
		return nil
	}

	if s.Size, err = cos.ParseSize(value, cos.UnitsIEC); err != nil {
		return fmt.Errorf("error parsing shard_size (accepts IEC, SI, or count formats): %w", err)
	}

	s.IsCount = false
	return nil
}

func (s *ShardSize) String() string {
	if s.IsCount {
		return fmt.Sprintf("%d files", s.Count)
	}
	return fmt.Sprintf("%d bytes", s.Size)
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
		return errors.New("invalid sort flag format")
	}

	alg.IsSet = true

	switch parts[0] {
	case "alpha", "alphanumeric":
		alg.Kind = "alphanumeric"
		if len(parts) > 1 {
			switch {
			case parts[1] == "inc":
				alg.Decreasing = false
			case parts[1] == "dec":
				alg.Decreasing = true
			default:
				return errors.New("invalid alphanumeric sort option, expected 'inc' or 'dec'")
			}
		}
	case "shuffle":
		alg.Kind = "shuffle"
		if len(parts) > 1 {
			alg.Seed = parts[1]
			if _, err := strconv.ParseInt(alg.Seed, 10, 64); err != nil {
				return errors.New("invalid shuffle seed, must be a valid integer")
			}
		}
	default:
		return errors.New("invalid sorting algorithm, expected 'alpha' or 'shuffle'")
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

///////////////////////
// Parse `-ekm` flag //
///////////////////////

type EKMFlag struct {
	IsSet     bool
	Path      string
	JSONBytes []byte
	Ekm       shard.ExternalKeyMap
}

func (e *EKMFlag) Set(param string) error {
	var err error
	param = strings.TrimSpace(param)
	if strings.HasSuffix(param, ".json") {
		e.Path = param
		e.JSONBytes, err = os.ReadFile(param)
		if err != nil {
			return fmt.Errorf("error reading ekm file: %w", err)
		}
	} else {
		e.JSONBytes = []byte(param)
	}

	var jsonContent map[string][]string
	if err := jsoniter.Unmarshal(e.JSONBytes, &jsonContent); err != nil {
		return fmt.Errorf("error unmarshal ekm file: %w", err)
	}

	e.IsSet = true

	e.Ekm = shard.NewExternalKeyMap(16)
	for format, samples := range jsonContent {
		for _, sample := range samples {
			if err = e.Ekm.Add(sample, format); err != nil {
				return fmt.Errorf("error parsing ekm file: %w", err)
			}
		}
	}
	return nil
}

func (e *EKMFlag) String() string {
	return e.Path
}

///////////////////////////
// Parse `-dry_run` flag //
///////////////////////////

type DryRunFlag struct {
	IsSet bool
	Mode  string
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

func (*DryRunFlag) IsBoolFlag() bool {
	return true
}

// Load configuration for ishard from CLI
func LoadFromCLI() (*Config, error) {
	cfg := DefaultConfig
	err := parseCliParams(&cfg)
	return &cfg, err
}

func parseCliParams(cfg *Config) error {
	flag.StringVar(&cfg.SrcBck.Name, "src_bck", "", "Source bucket name or URI.")
	flag.StringVar(&cfg.DstBck.Name, "dst_bck", "", "Destination bucket name or URI.")
	flag.StringVar(&cfg.ShardTemplate, "shard_template", "shard-%06d", "The template used for generating output shards. Default is `\"shard-%06d\"`. Accepts Bash, Fmt, or At formats.\n"+
		"  -shard_template=\"prefix-{0000..4096..8}-suffix\": Generate output shards prefix-0000-suffix, prefix-0008-suffix, prefix-0016-suffix, and so on.\n"+
		"  -shard_template=\"prefix-%06d-suffix\": Generate output shards prefix-000000-suffix, prefix-000001-suffix, prefix-000002-suffix, and so on.\n"+
		"  -shard_template=\"prefix-@00001-gap-@100-suffix\": Generate output shards prefix-00001-gap-001-suffix, prefix-00001-gap-002-suffix, and so on.")

	flag.StringVar(&cfg.Ext, "ext", ".tar", "Extension used for generating output shards. Default is `\".tar\"`. Options are \".tar\" | \".tgz\" | \".tar.gz\" | \".zip\" | \".tar.lz4\" formats.")
	flag.BoolVar(&cfg.Collapse, "collapse", false, "If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size. Default is `false`.")
	flag.BoolVar(&cfg.Progress, "progress", false, "If true, display the progress of processing objects in the source bucket. Default is `false`.")
	flag.Var(&cfg.DryRunFlag, "dry_run", "If set, only shows the layout of resulting output shards without actually executing archive jobs. Use -dry_run=\"show_keys\" to include sample keys.")
	flag.Var(&cfg.EKMFlag, "ekm", "Specify an external key map (EKM) to pack samples into shards based on customized regex categories, either as a JSON string or a path to a JSON file.\n"+
		"  -ekm=\"/path/to/ekm.json\"\n"+
		"  -ekm=\"{\\\"fish-%d.tar\\\": [\\\"train/n01440764.*\\\", \\\"train/n01443537.*\\\"], \\\"dog-%d.tar\\\": [\\\"train/n02084071.*\\\", \\\"train/n02085782.*\\\"]}\"")
	flag.Var(&cfg.SortFlag, "sort", "If set, sorting algorithm will be performed on files within shards\n"+
		"  -sort=\"alpha:inc\": Sorts the items in alphanumeric order in ascending (increasing) order.\n"+
		"  -sort=\"shuffle:124123\": Randomly shuffles the items using the specified seed 124123 for reproducibility. If the seed cannot be parsed as an integer, the flag is rejected.")

	var (
		err                 error
		sampleExts          string
		sampleKeyPatternStr string
		missingExtActStr    string
	)

	flag.Var(&cfg.ShardSize, "shard_size", "Approximate size of each output shard. Supports both count-based and size-based formats. Default is `\"1MiB\"`.\n"+
		"  -shard_size=\"10\": Sets the number of samples contained in each output shard to 10.\n"+
		"  -shard_size=\"16MiB\": Sets the size of each output shard to \"16MiB\" using the IEC format.\n"+
		"  -shard_size=\"4KB\": Sets the size of each output shard to \"4KB\" using the SI format.")
	flag.StringVar(&sampleExts, "sample_exts", "", "Comma-separated list of extensions that should exists in the dataset. e.g. -sample=\".JPEG,.xml,.json\". See -missing_extension_action for handling missing extensions")
	flag.StringVar(&sampleKeyPatternStr, "sample_key_pattern", "", "The pattern used to substitute source file names to sample keys. Default it `\"base_filename\"`. Options are \"base_file_name\" | \"full_name\" | \"collapse_all_dir\" | \"any other valid regex\" \n"+
		"This ensures that files with the same sample key are always sharded into the same output shard.\n"+
		"  -sample_key_pattern=\"base_filename\": The default option. Extracts and uses only the base filename as the sample key to merge. Removes all directory paths and extensions.\n"+
		"  -sample_key_pattern=\"full_name\": Performs no substitution, using the entire file name without extension as the sample key.\n"+
		"  -sample_key_pattern=\"collapse_all_dir\": Removes all '/' characters from the file name, using the resulting string as the sample key.\n"+
		"  -sample_key_pattern=\".*/([^/]+)/[^/]+$\": Applies a custom regex pattern to substitute the file names to their last level of directory names.")

	flag.StringVar(&missingExtActStr, "missing_extension_action", "ignore", "Specifies the action to take when an expected extension is missing from a sample. Default is `\"ignore\"`. Options are: \"abort\" | \"warn\" | \"ignore\" | \"exclude\".\n"+
		"  -missing_extension_action=\"ignore\": Do nothing when an expected extension is missing.\n"+
		"  -missing_extension_action=\"warn\": Print a warning if a sample contains an unspecified extension.\n"+
		"  -missing_extension_action=\"abort\": Stop the process if a sample contains an unspecified extension.\n"+
		"  -missing_extension_action=\"exclude\": Exclude any incomplete records and remove unnecessary extensions.")

	flag.Parse()

	var reactions = []string{"ignore", "warn", "abort", "exclude"}
	if !cos.StringInSlice(missingExtActStr, reactions) {
		msg := fmt.Sprintf("Invalid action: %s. Accepted values are: abort, warn, ignore, exclude\n", missingExtActStr)
		return errWithUsage(errors.New(msg))
	}

	if sampleExts != "" {
		cfg.MExtMgr, err = NewMissingExtManager(missingExtActStr, strings.Split(sampleExts, ","))
		if err != nil {
			return errWithUsage(err)
		}
	}

	var commonPatterns = map[string]SampleKeyPattern{
		"base_file_name":   BaseFileNamePattern,
		"full_name":        FullNamePattern,
		"collapse_all_dir": CollapseAllDirPattern,
	}

	if sampleKeyPatternStr == "" {
		fmt.Println("\"sample_key_pattern\" is not specified, use \"base_file_name\" as sample key by default.")
		cfg.SampleKeyPattern = BaseFileNamePattern
	} else if pattern, ok := commonPatterns[sampleKeyPatternStr]; ok {
		cfg.SampleKeyPattern = pattern
	} else {
		fmt.Printf("\"sample_key_pattern\" %s is not built-in (\"base_file_name\" | \"full_name\" | \"collapse_all_dir\"), compiled as custom regex\n", sampleKeyPatternStr)
		if _, err := regexp.Compile(sampleKeyPatternStr); err != nil {
			return errWithUsage(err)
		}
		cfg.SampleKeyPattern = SampleKeyPattern{Regex: sampleKeyPatternStr, CaptureGroup: "$1"}
	}

	if cfg.SrcBck.Name == "" || cfg.DstBck.Name == "" {
		return errWithUsage(errors.New("Error: src_bck and dst_bck are required parameters.\n%s"))
	}

	if cfg.SrcBck, cfg.SrcPrefix, err = cmn.ParseBckObjectURI(cfg.SrcBck.Name, cmn.ParseURIOpts{DefaultProvider: apc.AIS}); err != nil {
		return errWithUsage(err)
	}
	if cfg.DstBck, _, err = cmn.ParseBckObjectURI(cfg.DstBck.Name, cmn.ParseURIOpts{DefaultProvider: apc.AIS}); err != nil {
		return errWithUsage(err)
	}

	return nil
}

func errWithUsage(err error) error {
	var buf strings.Builder
	flag.CommandLine.SetOutput(&buf)
	flag.Usage()
	flag.CommandLine.SetOutput(os.Stderr)
	return fmt.Errorf("%v\n%s", err, buf.String())
}
